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

from pathlib import Path

import polars as pl

from tests.mbb_data_build._parity_helpers import assert_parquet_parity
from tests.mbb_data_build.conftest import oracle_path

KEYS = ["game_id", "game_play_number"]


def test_pbp_parity_full_2025(built_base):
    py = pl.read_parquet(built_base / "pbp" / "parquet" / "play_by_play_2025.parquet")
    oracle = oracle_path("pbp", "play_by_play")
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c not in KEYS]
    assert_parquet_parity(
        py,
        oracle,
        keys=KEYS,
        sample_cols=sample,
        # pbp column order is payload-first-seen; matches the NBA/WNBA
        # template's rationale (raw repo may have been re-scraped since the
        # oracle was compiled).
        require_order=False,
        # Deliberate improvement: R/jsonlite has no int64, so the released
        # `id` is Float64 (lossy for MBB's 18-digit ids); the Python producer
        # emits exact Int64.
        dtype_upgrades={"id": (pl.Int64(), pl.Float64())},
    )


def test_pbp_row_and_column_count_match_oracle(built_base):
    # Cheap, always-green sanity check independent of any per-row divergence.
    py = pl.read_parquet(built_base / "pbp" / "parquet" / "play_by_play_2025.parquet")
    r = pl.read_parquet(oracle_path("pbp", "play_by_play"))
    assert py.shape == r.shape
    assert set(py.columns) == set(r.columns)
