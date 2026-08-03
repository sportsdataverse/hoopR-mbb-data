"""Parity: Python team_box vs the R-released parquet oracle, FULL 2025 season.

Port provenance: ``hoopR:::helper_espn_mbb_team_box``. Oracle:
``hoopR-mbb-data/mbb/team_box/parquet/team_box_2025.parquet``, built from the
real sibling ``hoopR-mbb-raw`` checkout. ``largest_lead`` is a real per-game
payload field for MBB (part of the WBB-shared fixed column select), so unlike
NBA there is no season-level backfill to worry about.
"""

import polars as pl

from tests.mbb_data_build._parity_helpers import assert_parquet_parity
from tests.mbb_data_build.conftest import oracle_path

KEYS = ["game_id", "team_id"]


def test_team_box_parity_full_2025(built_base):
    py = pl.read_parquet(built_base / "team_box" / "parquet" / "team_box_2025.parquet")
    oracle = oracle_path("team_box", "team_box")
    all_cols = [c for c in pl.read_parquet_schema(str(oracle)) if c not in KEYS]
    assert_parquet_parity(py, oracle, keys=KEYS, sample_cols=all_cols)
