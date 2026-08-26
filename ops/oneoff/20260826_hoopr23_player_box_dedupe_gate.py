"""Gate the hoopR#23 player_box dual-team dedupe rebuild (2026-08-26).

Compares each rebuilt (worktree) player_box_{y}.parquet against the HEAD
baseline (pre-fix committed parquet) and asserts, per season:

  (a) dupe invariant: no (game_id, athlete_id) under >1 team_id in the rebuilt
      frame (athlete_id nulls excluded -- distinct unknown players);
  (b) row delta: rows dropped == baseline dual-team pairs (exactly one row
      removed per pair);
  (c) no legitimate rows lost: every removed (game_id, athlete_id, team_id)
      key had a twin on the other team in the baseline, that twin survives,
      and every surviving row is byte-identical to its baseline row.

Plus the ground-truth spot-check on 2021 game 401253901 (raw-payload
evidence: 5 Oklahoma State players duplicated into the Texas Tech block).

Run AFTER the rebuild and BEFORE committing the rebuilt outputs (the
baseline is read from git HEAD):

    uv run python ops/oneoff/20260826_hoopr23_player_box_dedupe_gate.py \
        2014 2015 2017 2018 2020 2021 2022 2025
"""

from __future__ import annotations

import io
import subprocess
import sys

import polars as pl

KEY = ["game_id", "athlete_id", "team_id"]


def dupe_pairs(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.filter(pl.col("athlete_id").is_not_null())
        .group_by(["game_id", "athlete_id"])
        .agg(pl.col("team_id").n_unique().alias("nt"))
        .filter(pl.col("nt") > 1)
        .select(["game_id", "athlete_id"])
    )


def head_parquet(rel: str) -> pl.DataFrame:
    # git show -> stdout bytes -> BytesIO: no temp file (Windows denies
    # unlinking a parquet polars still has memory-mapped).
    out = subprocess.run(["git", "show", f"HEAD:{rel}"], capture_output=True, check=True)
    return pl.read_parquet(io.BytesIO(out.stdout))


def gate_season(y: int) -> list[str]:
    fails: list[str] = []
    rel = f"mbb/player_box/parquet/player_box_{y}.parquet"
    old = head_parquet(rel)
    new = pl.read_parquet(rel)

    pairs_old = dupe_pairs(old)
    pairs_new = dupe_pairs(new)
    dropped = old.height - new.height
    print(
        f"{y}: baseline rows={old.height} pairs={pairs_old.height} | "
        f"rebuilt rows={new.height} pairs={pairs_new.height} | dropped={dropped}"
    )
    if pairs_new.height != 0:
        fails.append(f"{y}: (a) rebuilt frame still has {pairs_new.height} dual-team pairs")
    if dropped != pairs_old.height:
        fails.append(f"{y}: (b) dropped {dropped} rows, expected {pairs_old.height}")

    # column ORDER may legitimately differ (the rebuilt frame follows the
    # CURRENT sdv-py _MBB_FINAL_ORDER); the column SET must not.
    if set(old.columns) != set(new.columns):
        fails.append(f"{y}: column set changed: {set(old.columns) ^ set(new.columns)}")
        return fails
    # athlete_id-null rows are never dedupe candidates and never match in a
    # key join (polars joins don't match nulls) -- compare their count, and
    # exclude them from the key-level anti-joins below.
    if old["athlete_id"].null_count() != new["athlete_id"].null_count():
        fails.append(
            f"{y}: (c) null-athlete rows changed "
            f"{old['athlete_id'].null_count()} -> {new['athlete_id'].null_count()}"
        )
    old_nn = old.filter(pl.col("athlete_id").is_not_null())
    new_nn = new.filter(pl.col("athlete_id").is_not_null())
    removed = old_nn.select(KEY).join(new_nn.select(KEY), on=KEY, how="anti")
    if removed.height != pairs_old.height:
        fails.append(f"{y}: (c) removed {removed.height} keys, expected {pairs_old.height}")
    # each removed key had a twin on the other team, and the twin survives
    twins = removed.join(
        old_nn.select(KEY).rename({"team_id": "twin_team_id"}),
        on=["game_id", "athlete_id"],
        how="inner",
    ).filter(pl.col("team_id") != pl.col("twin_team_id"))
    if twins.select(["game_id", "athlete_id"]).unique().height != removed.height:
        fails.append(f"{y}: (c) some removed keys had no other-team twin in baseline")
    orphaned = (
        twins.select(["game_id", "athlete_id", "twin_team_id"])
        .rename({"twin_team_id": "team_id"})
        .join(new_nn.select(KEY), on=KEY, how="anti")
    )
    if orphaned.height:
        fails.append(f"{y}: (c) {orphaned.height} kept twins missing from rebuilt frame")

    # surviving rows value-identical to baseline: multiset comparison (sort by
    # every column) with the baseline projected into the rebuilt column order,
    # so a legitimate column reorder or null-key row order can't false-fail.
    # Known benign drift folded out: athlete_position_abbreviation null vs the
    # literal "NA" ESPN ships for position-less athletes (2014/2015 payloads) --
    # the current sdv-py helper passes "NA" through where the old build nulled
    # it; orthogonal to the dedupe.
    def _norm(df: pl.DataFrame) -> pl.DataFrame:
        if "athlete_position_abbreviation" in df.columns:
            df = df.with_columns(pl.col("athlete_position_abbreviation").replace({"NA": None}))
        return df

    kept_old = _norm(old.join(removed, on=KEY, how="anti").select(new.columns))
    if not kept_old.sort(new.columns).equals(_norm(new).sort(new.columns)):
        fails.append(f"{y}: (c) surviving rows differ from baseline beyond the dedupe")
    return fails


def spot_check_401253901() -> list[str]:
    fails: list[str] = []
    df = pl.read_parquet("mbb/player_box/parquet/player_box_2021.parquet").filter(
        pl.col("game_id") == 401253901
    )
    counts = dict(df.group_by("team_id").len().iter_rows())
    if counts != {2641: 12, 197: 16}:
        fails.append(f"401253901: team row counts {counts}, expected {{2641: 12, 197: 16}}")
    osu = {4432166, 4432255, 4432851, 4433163, 4592267}
    rows = df.filter(pl.col("athlete_id").is_in(list(osu)))
    if rows.height != 5 or set(rows["team_id"].to_list()) != {197}:
        fails.append("401253901: the 5 duplicated OSU athletes are not exactly-once on team 197")
    starters = rows.filter(pl.col("starter") == True)["athlete_id"].to_list()  # noqa: E712
    if sorted(starters) != [4432166, 4432851, 4433163]:
        fails.append(f"401253901: OSU dupe starters {sorted(starters)} != expected 3")
    return fails


def main(argv: list[str]) -> int:
    seasons = [int(a) for a in argv] or [2014, 2015, 2017, 2018, 2020, 2021, 2022, 2025]
    fails: list[str] = []
    for y in seasons:
        fails += gate_season(y)
    if 2021 in seasons:
        fails += spot_check_401253901()
    if fails:
        print("\nGATE FAILED:")
        for f in fails:
            print(" -", f)
        return 1
    print("\nGATE PASSED for seasons:", seasons)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
