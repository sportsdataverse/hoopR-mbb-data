"""Committed Bart Torvik team lists and schedules for the crosswalk builds.

barttorvik.com's CloudFront answers 403 to every datacenter IP -- GitHub
runners and the droplet alike -- and 200 only to residential ones. The team
crosswalk needs one Torvik file per season, ``{season}_team_results.csv`` (its
``team`` / ``conf`` columns are the Torvik join). The schedule crosswalk also
needs ``{season}_super_sked.json``, Torvik's game list, which is the spine it
joins ESPN games onto. Both are fetched from a residential machine and committed
under ``torvik/`` at the repo root, with URL, fetch time and sha256 in
``torvik/manifest.json``. The builds read the committed files and never call
barttorvik.com.

Refresh (residential IP only; before the season starts, and again once the
conference and NCAA tournament games are scheduled, since the schedule snapshot
holds only the games Torvik listed when it was fetched)::

    cd python && uv run python -m mbb_data_build.torvik_snapshot -s 2027 -e 2027
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from sportsdataverse._crosswalk_basketball_sources import TORVIK_FIRST_SEASON, parse_super_sked
from sportsdataverse.mbb.torvik_parsers import parse_torvik_csv

from mbb_data_build._logging import get_logger

log = get_logger()

ROOT = Path(__file__).resolve().parents[2] / "torvik"
URL = "https://barttorvik.com/{season}_team_results.csv"
SKED_URL = "https://barttorvik.com/{season}_super_sked.json"


def _teams(text: str, what: str) -> pl.DataFrame:
    """``team`` / ``conf`` from a team_results CSV, refusing one with no teams."""
    raw = parse_torvik_csv(text)
    if not {"team", "conf"} <= set(raw.columns) or raw["team"].drop_nulls().len() == 0:
        raise ValueError(f"{what}: no Torvik team rows ({raw.height} rows)")
    return raw.select(pl.col("team").cast(pl.Utf8), pl.col("conf").cast(pl.Utf8))


def bart_teams(season: int) -> pl.DataFrame:
    """The season's Torvik ``team`` / ``conf`` from the committed snapshot.

    The same frame sdv-py's ``torvik_teams("mbb", season)`` fetches live, for
    ``mbb_team_crosswalk(bart=...)``. Zero rows before Torvik's first season,
    as sdv-py returns.

    Raises:
        FileNotFoundError: No snapshot for a season Torvik covers.
    """
    if season < TORVIK_FIRST_SEASON["mbb"]:
        return pl.DataFrame(schema={"team": pl.Utf8, "conf": pl.Utf8})
    path = ROOT / f"{season}_team_results.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} missing; from a residential IP run "
            f"`cd python && uv run python -m mbb_data_build.torvik_snapshot -s {season} -e {season}` and commit it"
        )
    return _teams(path.read_text(encoding="utf-8"), str(path))


def _games(text: str, what: str, season: int) -> pl.DataFrame:
    """Dated games from a super_sked JSON, refusing one with none."""
    games = parse_super_sked(text, season).filter(pl.col("game_date").is_not_null())
    if games.height == 0:
        raise ValueError(f"{what}: no dated Torvik games")
    return games


def bart_games(season: int) -> pl.DataFrame | None:
    """The season's Torvik super schedule from the committed snapshot, or ``None`` when none is committed.

    The same frame sdv-py's ``bart_super_sked("mbb", season)`` fetches live, for
    ``mbb_schedule_crosswalk(bart_games=...)``.
    """
    path = ROOT / f"{season}_super_sked.json"
    return _games(path.read_text(encoding="utf-8"), str(path), season) if path.exists() else None


def schedule_inputs(season: int) -> dict[str, pl.DataFrame]:
    """``bart`` / ``bart_games`` for ``mbb_schedule_crosswalk`` from the committed snapshot.

    Empty when the season has no committed schedule, so sdv-py fetches both live
    as it always did -- which works from a residential IP only. A committed
    schedule without its team list raises (see :func:`bart_teams`).
    """
    games = bart_games(season)
    return {} if games is None else {"bart": bart_teams(season), "bart_games": games}


def fill_bart_winner(xwalk: pl.DataFrame, season: int, base: Path) -> pl.DataFrame:
    """Fill a null ``bart_winner`` from the ESPN result, for games finished after the schedule snapshot.

    The snapshot freezes Torvik's ``winner`` when it was fetched, so a game played since then has
    none (Torvik leaves it ``""``). Torvik's winner is always ``bart_team1`` or ``bart_team2`` (2026: 5,662 of 5,662 MBB
    rows), so it is whichever of the two the committed team crosswalk maps to ESPN's winning team,
    or the other one when only the loser maps. A row that already has a winner, an unfinished game
    and a pair neither of whose names maps are left as they are. Without the committed schedule and
    team crosswalk for the season, the frame comes back unchanged.
    """
    from mbb_data_build.alt_fill import _tree

    sched_path, team_path = _tree("schedules", season, base), _tree("team_crosswalk", season, base)
    if xwalk.height == 0 or not (sched_path.exists() and team_path.exists()):
        return xwalk
    home_won = pl.col("home_winner") == True  # noqa: E712 -- explicit bool mask (null stays unmatched)
    won = (
        pl.read_parquet(
            sched_path,
            columns=["game_id", "home_id", "away_id", "home_winner", "status_type_completed"],
        )
        .filter((pl.col("status_type_completed") == True) & pl.col("home_winner").is_not_null())  # noqa: E712
        .select(
            pl.col("game_id").cast(pl.Int64).cast(pl.Utf8).alias("espn_game_id"),
            pl.when(home_won).then(pl.col("home_id")).otherwise(pl.col("away_id")).alias("_win"),
            pl.when(home_won).then(pl.col("away_id")).otherwise(pl.col("home_id")).alias("_lose"),
        )
        .unique("espn_game_id", keep="first")
    )
    names = (
        pl.read_parquet(team_path, columns=["bart_team", "espn_team_id"])
        .drop_nulls()
        .unique("bart_team", keep="first")
    )
    if xwalk.schema["espn_game_id"] != won.schema["espn_game_id"]:
        raise TypeError(
            f"espn_game_id dtype {xwalk.schema['espn_game_id']} != schedule {won.schema['espn_game_id']}"
        )
    t1 = names.rename({"bart_team": "bart_team1", "espn_team_id": "_t1"})
    t2 = names.rename({"bart_team": "bart_team2", "espn_team_id": "_t2"})
    derived = (
        pl.when(pl.col("_t1") == pl.col("_win"))
        .then(pl.col("bart_team1"))
        .when(pl.col("_t2") == pl.col("_win"))
        .then(pl.col("bart_team2"))
        .when(pl.col("_t1") == pl.col("_lose"))
        .then(pl.col("bart_team2"))
        .when(pl.col("_t2") == pl.col("_lose"))
        .then(pl.col("bart_team1"))
    )
    out = (
        xwalk.join(won, on="espn_game_id", how="left", maintain_order="left")
        .join(t1, on="bart_team1", how="left", maintain_order="left")
        .join(t2, on="bart_team2", how="left", maintain_order="left")
        # Torvik leaves an unplayed game's winner empty (""), not null: treat both as missing,
        # and keep the original blank where ESPN has no result either.
        .with_columns(
            pl.coalesce(
                pl.when(pl.col("bart_winner") != "").then(pl.col("bart_winner")),
                derived,
                pl.col("bart_winner"),
            ).alias("bart_winner")
        )
        .drop("_win", "_lose", "_t1", "_t2")
    )
    log.info(
        "schedule_crosswalk %s: bart_winner filled from ESPN on %d rows",
        season,
        (out["bart_winner"] != xwalk["bart_winner"]).fill_null(True).sum(),
    )
    return out


def refresh(seasons: range) -> None:
    """Fetch each season's team_results.csv and super_sked.json into ``torvik/`` and record their provenance."""
    from sportsdataverse.mbb.torvik import torvik_ratings
    from sportsdataverse.mbb.torvik_runtime import _get

    manifest_path = ROOT / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    ROOT.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for season in seasons:
        # Fetch and validate both files before writing either, so a failed fetch leaves the
        # committed pair and its manifest entries as they were.
        # Torvik serves CRLF; git stores LF, so hash and write what is committed.
        text = torvik_ratings(year=season, return_parsed=False).replace("\r\n", "\n")
        n = _teams(text, URL.format(season=season)).height  # never commit a blocked/empty body
        # The runtime hands back parsed JSON; commit it compact, one line, as hashed.
        sked = json.dumps(_get(SKED_URL.format(season=season)), separators=(",", ":"))
        g = _games(sked, SKED_URL.format(season=season), season).height
        for name, url, body, count in (
            (f"{season}_team_results.csv", URL, text, {"teams": n}),
            (f"{season}_super_sked.json", SKED_URL, sked, {"games": g}),
        ):
            (ROOT / name).write_text(body, encoding="utf-8", newline="")
            manifest[name] = {
                "url": url.format(season=season),
                "fetched_at_utc": stamp,
                "sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
                **count,
            }
        log.info("torvik %s: %d teams, %d games", season, n, g)
    manifest_path.write_text(json.dumps(dict(sorted(manifest.items())), indent=2) + "\n")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("-s", "--start", type=int, required=True)
    p.add_argument("-e", "--end", type=int, required=True)
    a = p.parse_args(argv)
    refresh(range(max(a.start, TORVIK_FIRST_SEASON["mbb"]), a.end + 1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
