"""Parity: rosters / team_season_stats / player_season_stats / standings vs
the R-released parquet oracles, FULL 2025 season.

Port provenance: the script-local parsers in
``hoopR-mbb-data/R/espn_mbb_0{4,5,6,7}_*_creation.R``. ``player_season_stats``
is the MBB/NBA-shared builder (identity backfilled from the season's built
player_box, not team rosters -- see ``reshapers.player_season_stats_builder``);
the other three delegate to the shared WBB implementation after league
normalization. Long-format frames have no compact unique key, so ALL columns
act as sort keys (total order; duplicate rows compare as multisets).
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest
from mbb_data_build import ingest

from tests.mbb_data_build._parity_helpers import assert_parquet_parity
from tests.mbb_data_build.conftest import RAW_ROOT, oracle_path


@pytest.mark.parametrize(
    ("dataset", "stem", "keys"),
    [
        ("rosters", "rosters", ["team_id", "athlete_id"]),
        ("team_season_stats", "team_season_stats", None),
        ("standings", "standings", None),
    ],
)
def test_season_dataset_parity_full_2025(dataset, stem, keys, built_base):
    py = pl.read_parquet(built_base / dataset / "parquet" / f"{stem}_2025.parquet")
    oracle = oracle_path(dataset, stem)
    all_cols = list(pl.read_parquet_schema(str(oracle)))
    keys = keys if keys is not None else all_cols
    sample = [c for c in all_cols if c not in keys]
    # The producer writes rosters.athlete_id as Int32 (io.write_dataset). A committed
    # season not yet republished still carries String: pin that pair. Once it
    # is republished as Int32, no upgrade applies and dtypes must match exactly.
    legacy = dataset == "rosters" and pl.read_parquet_schema(str(oracle))["athlete_id"] == pl.String
    upgrades = {"athlete_id": (pl.Int32, pl.String)} if legacy else None
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample, dtype_upgrades=upgrades)


# Identity columns. The released 2025 file predates two upstream changes, so the
# fresh build is checked against their sources instead of against the release:
# * jersey / position / team_display_name come from the athlete's latest-dated
#   player_box row (sportsdataverse-py #1194; R's build_identity_lookup sorts the
#   same way). The release kept a frame-order pick for 128 athletes.
# * team_slug comes from the raw season-stats payload, which ESPN has since
#   re-slugged for renamed schools (e.g. saint-francis-red-flash -> -red-wolves).
# Every other column must match the release exactly, for every athlete.
_IDENTITY_FROM_PLAYER_BOX = ("athlete_jersey", "athlete_position_abbreviation", "team_display_name")
_IDENTITY_COLS = (*_IDENTITY_FROM_PLAYER_BOX, "team_slug")


def _split_count_names(df: pl.DataFrame) -> list[str]:
    """Stat names of the numeric count rows sportsdataverse-py #1201 derives.

    Each ``fooMade-fooAttempted`` text row is followed by numeric ``fooMade``
    and ``fooAttempted`` rows. Releases published before #1201 lack them.
    """
    names = df.get_column("stat_name").drop_nulls().unique().to_list()
    return sorted({part for n in names if n.count("-") == 1 for part in n.split("-")})


def _without_split_counts(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(~pl.col("stat_name").is_in(_split_count_names(df)))


def _built(built_base: Path, dataset: str) -> pl.DataFrame:
    return pl.read_parquet(built_base / dataset / "parquet" / f"{dataset}_2025.parquet")


def test_player_season_stats_split_counts_match_their_text_2025(built_base):
    # Both pipelines emit the #1201 count rows (espn_mbb_05 via split_made_attempted).
    py = _built(built_base, "player_season_stats")
    split_names = _split_count_names(py)
    assert split_names, "expected the #1201 made/attempted count rows"

    derived = py.filter(pl.col("stat_name").is_in(split_names))
    parents = py.filter(pl.col("stat_name").str.count_matches("-") == 1)
    assert derived.height == 2 * parents.height

    # each derived count equals its half of the parent's "made-attempted" text
    key = ["athlete_id", "team_id", "category"]
    halves = parents.with_columns(
        _name=pl.col("stat_name").str.split("-"),
        _val=pl.col("display_value").str.split("-"),
    )
    got = derived.select(*key, sn=pl.col("stat_name"), v=pl.col("value"))
    for i in (0, 1):
        expected = halves.select(
            *key,
            sn=pl.col("_name").list.get(i),
            v=pl.col("_val").list.get(i).cast(pl.Float64),
        )
        missing = expected.join(got, on=[*key, "sn", "v"], how="anti")
        assert missing.is_empty(), f"derived counts disagree with text:\n{missing.head()}"


def test_player_season_stats_parity_full_2025(built_base):
    py = _without_split_counts(_built(built_base, "player_season_stats"))
    oracle = oracle_path("player_season_stats", "player_season_stats")
    r = _without_split_counts(pl.read_parquet(oracle))
    cols = [c for c in r.columns if c not in _IDENTITY_COLS]
    assert set(_IDENTITY_COLS) <= set(r.columns)
    assert_parquet_parity(
        py.select(cols),
        # assert_parquet_parity re-reads from a path, so write the projected
        # oracle to a temp file rather than teach the helper a frame input.
        _write_tmp(r.select(cols)),
        keys=cols,
        sample_cols=[],
    )


def test_player_season_stats_identity_is_the_latest_player_box_row_2025(built_base):
    py = _built(built_base, "player_season_stats")
    pb = _built(built_base, "player_box")
    ident = ["athlete_id", "athlete_display_name", *_IDENTITY_FROM_PLAYER_BOX]
    got = py.select(ident).unique()
    assert got.get_column("athlete_id").is_unique().all(), "one identity per athlete"
    # every row on the athlete's latest game date is acceptable: a same-day doubleheader
    # for two teams (5116992, 2024-11-04) has no later game to break the tie.
    latest = pb.filter(pl.col("game_date") == pl.col("game_date").max().over("athlete_id"))
    off = got.join(latest.select(ident).unique(), on=ident, how="anti", nulls_equal=True)
    assert off.is_empty(), f"identity is not from the latest-dated player_box row:\n{off.head()}"


def test_player_season_stats_team_slug_is_the_raw_payload_slug_2025(built_base):
    py = _built(built_base, "player_season_stats").select("athlete_id", "team_slug").unique()
    r = pl.read_parquet(oracle_path("player_season_stats", "player_season_stats"))
    moved = py.join(
        r.select("athlete_id", "team_slug").unique(),
        on=["athlete_id", "team_slug"],
        how="anti",
        nulls_equal=True,
    )
    assert moved.height < 100, f"{moved.height} athletes changed team_slug; expected a few renames"
    for aid, slug in moved.iter_rows():
        payload = ingest.read_final(aid, raw_root=RAW_ROOT, subdir="player_season_stats/json")
        assert payload is not None, aid
        slugs = {
            s.get("teamSlug")
            for cat in payload.get("categories", [])
            for s in cat.get("statistics", [])
        }
        assert slug in slugs, f"{aid}: {slug!r} not in its raw payload {sorted(map(str, slugs))}"


def _write_tmp(df: pl.DataFrame) -> Path:
    p = Path(tempfile.mkdtemp(prefix="pss_oracle_")) / "filtered.parquet"
    df.write_parquet(p)
    return p
