"""Parity: Python player_box vs the R-released parquet oracle, FULL 2025 season.

Port provenance: ``hoopR:::helper_espn_mbb_player_box``. Oracle:
``hoopR-mbb-data/mbb/player_box/parquet/player_box_2025.parquet``, built from
the real sibling ``hoopR-mbb-raw`` checkout. No ``plus_minus`` column (MBB
never carries it, same as WBB).

ESPN sometimes double-lists an athlete on BOTH teams' boxscores (e.g. 2025
game 401719238 had 10 such athletes). Since 2026-08-26 the builder's
``dedupe_player_box_dual_team`` season postprocess (hoopR#23) drops the
wrong-team copy, so ``(game_id, athlete_id)`` is unique in both the Python
build and the (rebuilt) oracle; ``team_id`` stays in the sort key as a
belt-and-braces tie-breaker.
"""

import polars as pl

from tests.mbb_data_build._parity_helpers import assert_parquet_parity
from tests.mbb_data_build.conftest import oracle_path

KEYS = ["game_id", "athlete_id", "team_id"]


def test_player_box_parity_full_2025(built_base):
    py = pl.read_parquet(built_base / "player_box" / "parquet" / "player_box_2025.parquet")
    oracle = oracle_path("player_box", "player_box")
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c not in KEYS]
    assert_parquet_parity(py, oracle, keys=KEYS, sample_cols=sample)
