"""Parity: Python game_rosters + officials vs the R-released parquet oracles,
FULL 2025 season.

Port provenance: script-local parsers in
``hoopR-mbb-data/R/espn_mbb_09_game_rosters_creation.R`` and
``espn_mbb_10_officials_creation.R``. MBB delta (same as NBA): officials have
no dedicated raw directory -- both datasets read the SAME
``game_rosters/json`` sidecar (``reshapers._officials_builder``);
``officials.game_id`` is Int32 (R ``safe_int``), diverging from the
WBB/WNBA officials release's String.
"""

import polars as pl
import pytest

from tests.mbb_data_build._parity_helpers import assert_parquet_parity
from tests.mbb_data_build.conftest import oracle_path


@pytest.mark.parametrize(
    ("dataset", "stem", "keys"),
    [
        ("game_rosters", "game_rosters", ["game_id", "athlete_id"]),
        ("officials", "officials", ["game_id", "official_order"]),
    ],
)
def test_sidecar_parity_full_2025(dataset, stem, keys, built_base):
    py = pl.read_parquet(built_base / dataset / "parquet" / f"{stem}_2025.parquet")
    oracle = oracle_path(dataset, stem)
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c not in keys]
    # The producer writes game_rosters.game_id as Int32 (io.write_dataset). A committed
    # season not yet republished still carries String: pin that pair. Once it
    # is republished as Int32, no upgrade applies and dtypes must match exactly.
    legacy = (
        dataset == "game_rosters" and pl.read_parquet_schema(str(oracle))["game_id"] == pl.String
    )
    upgrades = {"game_id": (pl.Int32, pl.String)} if legacy else None
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample, dtype_upgrades=upgrades)
