"""Parity: Python player_box vs the R-released parquet oracle, FULL 2025 season.

Port provenance: ``hoopR:::helper_espn_mbb_player_box``. Oracle:
``hoopR-mbb-data/mbb/player_box/parquet/player_box_2025.parquet``, built from
the real sibling ``hoopR-mbb-raw`` checkout. No ``plus_minus`` column (MBB
never carries it, same as WBB).

``(game_id, athlete_id)`` is NOT quite a unique key for MBB: one 2025 game
(401719238) has 10 athletes double-listed on BOTH teams' boxscores (a
genuine ESPN data quirk, present identically in both the Python build and
the oracle). Sorting on the 2-column key alone leaves those duplicate pairs
in whatever relative order each frame happened to build them in, which can
legitimately differ and produce a false positional mismatch. ``team_id`` is
appended as a tie-breaker since ``(game_id, athlete_id, team_id)`` IS unique
in both frames.
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
