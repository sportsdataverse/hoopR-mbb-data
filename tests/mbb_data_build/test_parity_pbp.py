"""Parity: Python play_by_play vs the R-released parquet oracle, FULL 2025 season.

Port provenance: ``hoopR:::helper_espn_mbb_pbp``
(``hoopR-mbb-data/R/espn_mbb_01_pbp_creation.R``). Oracle:
``hoopR-mbb-data/mbb/pbp/parquet/play_by_play_2025.parquet`` -- the full
committed season asset, built from the real sibling ``hoopR-mbb-raw``
checkout (not a 3-game fixture).

MBB delta vs the NBA sibling: the play ``id`` is an 18-digit ESPN
concatenation that overflows R/jsonlite's double mantissa in a much bigger
way than NBA's -- ~41% of 2025 rows collide in the released Float64. The
Python producer reads ``id`` as exact Int64 straight from the payload (the
same deliberate dtype improvement as NBA/WBB/WNBA, #245); parity is asserted
through the oracle's lossy Float64 view (cast py's Int64 DOWN to Float64,
not the oracle cast up) so the comparison is apples-to-apples even where
the oracle itself can't disambiguate two different ids.
"""


import polars as pl

from tests.mbb_data_build._parity_helpers import assert_parquet_parity
from tests.mbb_data_build.conftest import oracle_path

KEYS = ["game_id", "game_play_number"]

NAME_COLS = ("athlete_name_1", "athlete_name_2", "athlete_name_3")


def test_pbp_parity_full_2025(built_base):
    py = pl.read_parquet(built_base / "pbp" / "parquet" / "play_by_play_2025.parquet")
    # Additive name columns (2026-07): joined per game from boxscore.players.
    # The only legitimate misses are non-players (e.g. coach technicals).
    has_id = py.filter(pl.col("athlete_id_1").is_not_null())
    matched = has_id.filter(pl.col("athlete_name_1").is_not_null()).height
    assert matched >= 0.99 * has_id.height, (
        f"athlete_name_1 resolved on {matched}/{has_id.height} id-bearing rows"
    )
    oracle = oracle_path("pbp", "play_by_play")
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c not in KEYS]
    assert_parquet_parity(
        py,
        oracle,
        keys=KEYS,
        sample_cols=sample,
        py_only_additive=NAME_COLS,
        # pbp column order is payload-first-seen; matches the NBA/WNBA
        # template's rationale (raw repo may have been re-scraped since the
        # oracle was compiled).
        require_order=False,
        # No dtype_upgrades pin: since the 2026-07 python republish, the
        # committed tree oracle is itself python-built with exact Int64 `id`
        # (the R-era Float64 lossiness is gone), so id compares directly.
    )


def test_pbp_row_and_column_count_match_oracle(built_base):
    # Cheap, always-green sanity check independent of any per-row divergence.
    py = pl.read_parquet(built_base / "pbp" / "parquet" / "play_by_play_2025.parquet")
    r = pl.read_parquet(oracle_path("pbp", "play_by_play"))
    assert py.shape == r.shape
    assert set(py.columns) == set(r.columns)
