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

from tests.mbb_data_build._parity_helpers import assert_parquet_parity
from tests.mbb_data_build.conftest import oracle_path


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
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample)


# Two athlete_ids (out of ~13,000 in the 2025 season) carry raw-data ambiguity
# that makes their identity-lookup "last occurrence wins" dedup order-
# dependent, confirmed by direct inspection (not swallowed as a generic
# tolerance):
#
# * 5116992 plays two DIFFERENT teams' games on the SAME calendar date
#   (2024-11-04, games 401720557 and 401720867) -- a genuine same-day
#   scheduling/ID overlap in the raw ESPN data. Which of the two rows the
#   "last occurrence in player_box frame order" dedup picks is a tie-break,
#   and the tie-break order is not guaranteed to agree between the R and
#   Python pipelines (parallel per-game compiles with no shared canonical
#   order for same-date games).
# * 5177701's raw ``player_season_stats/json`` payload tags its 2025-season
#   ``totals``/``averages`` entries with ``teamSlug="cleveland-state-vikings"``
#   / ``teamId=325`` -- exactly what this Python build emits, and what the
#   athlete's own player_box rows show all season. The R oracle instead
#   carries `team_display_name="Defiance Yellow Jackets"` (a different
#   program entirely) with a null jersey -- i.e. the ORACLE's identity
#   lookup resolved to a different, seemingly wrong player_box row for this
#   athlete_id. Bug-matching a release that disagrees with its own source
#   payload would hide a real divergence rather than document it.
_KNOWN_COLLIDING_ATHLETE_IDS = (5116992, 5177701)


def test_player_season_stats_parity_full_2025(built_base):
    py = pl.read_parquet(
        built_base / "player_season_stats" / "parquet" / "player_season_stats_2025.parquet"
    )
    oracle = oracle_path("player_season_stats", "player_season_stats")
    r = pl.read_parquet(oracle)
    keys = list(r.columns)
    exclude = pl.col("athlete_id").is_in(_KNOWN_COLLIDING_ATHLETE_IDS)
    assert_parquet_parity(
        py.filter(~exclude),
        # assert_parquet_parity re-reads from a path, so write the filtered
        # oracle to a temp file rather than teach the helper a frame input.
        _write_tmp(r.filter(~exclude)),
        keys=keys,
        sample_cols=[],
    )


def test_player_season_stats_known_collisions_are_exactly_the_documented_two(built_base):
    # Cheap regression guard: if this ever grows beyond the two documented
    # ids, the exclusion above is silently swallowing a NEW divergence.
    py = pl.read_parquet(
        built_base / "player_season_stats" / "parquet" / "player_season_stats_2025.parquet"
    )
    r = pl.read_parquet(oracle_path("player_season_stats", "player_season_stats"))
    cols = list(r.columns)
    diff = py.select(cols).join(r, on=cols, how="anti", nulls_equal=True)
    assert set(diff.get_column("athlete_id").unique().to_list()) <= set(
        _KNOWN_COLLIDING_ATHLETE_IDS
    )


def _write_tmp(df: pl.DataFrame) -> Path:
    p = Path(tempfile.mkdtemp(prefix="pss_oracle_")) / "filtered.parquet"
    df.write_parquet(p)
    return p
