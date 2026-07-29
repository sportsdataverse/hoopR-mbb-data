"""Parity: Python shots vs the R-released parquet oracle, FULL 2025 season.

Port provenance: the shots block of ``espn_mbb_01_pbp_creation.R`` (filter
``shooting_play == TRUE`` on the compiled season pbp, project the 15 shot
columns). Oracle: ``hoopR-mbb-data/mbb/shots/parquet/shots_2025.parquet``,
derived from the full 2025 pbp build. The shot rows have no unique key, so
ALL columns are sort keys (total order; duplicate rows compare fine as
multisets).
"""

import polars as pl

from tests.mbb_data_build._parity_helpers import assert_parquet_parity
from tests.mbb_data_build.conftest import oracle_path


ADDITIVE_COLS = (
    "athlete_name_1",
    "athlete_name_2",
    "team_name",
    "team_mascot",
    "team_abbrev",
)


def test_shots_parity_full_2025(built_base):
    py = pl.read_parquet(built_base / "shots" / "parquet" / "shots_2025.parquet")
    # Additive columns (2026-07). Unlike the pro feeds, college pbp carries a
    # sliver of unattributed shots (no athlete_id_1) plus the odd shooter
    # missing from the game's boxscore -- so assert id-bearing coverage, not
    # a blanket zero (2025: 139 null names of 936k shots, 107 of them id-less).
    has_id = py.filter(pl.col("athlete_id_1").is_not_null())
    named = has_id.filter(pl.col("athlete_name_1").is_not_null()).height
    assert named >= 0.999 * has_id.height, (
        f"athlete_name_1 resolved on {named}/{has_id.height} id-bearing shots"
    )
    assert py["team_abbrev"].null_count() == 0
    oracle = oracle_path("shots", "shots")
    keys = list(pl.read_parquet_schema(str(oracle)))
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=[], py_only_additive=ADDITIVE_COLS)
