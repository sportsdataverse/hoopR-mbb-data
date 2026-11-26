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
    # The producer writes rosters.athlete_id as Int32 (io.write_dataset). A committed
    # season not yet republished still carries String: pin that pair. Once it
    # is republished as Int32, no upgrade applies and dtypes must match exactly.
    legacy = dataset == "rosters" and pl.read_parquet_schema(str(oracle))["athlete_id"] == pl.String
    upgrades = {"athlete_id": (pl.Int32, pl.String)} if legacy else None
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample, dtype_upgrades=upgrades)


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


def _split_count_names(df: pl.DataFrame) -> list[str]:
    """Stat names of the numeric count rows sportsdataverse-py #1201 derives.

    Each ``fooMade-fooAttempted`` text row is followed by numeric ``fooMade``
    and ``fooAttempted`` rows. The R oracle predates them.
    """
    names = df.get_column("stat_name").drop_nulls().unique().to_list()
    return sorted({part for n in names if n.count("-") == 1 for part in n.split("-")})


def _without_split_counts(py: pl.DataFrame) -> pl.DataFrame:
    return py.filter(~pl.col("stat_name").is_in(_split_count_names(py)))


def test_player_season_stats_split_counts_match_their_text_2025(built_base):
    # The released oracle predates the #1201 count rows (espn_mbb_05 now emits them via
    # split_made_attempted); assert them against their text, parity tests below run on the rest.
    py = pl.read_parquet(
        built_base / "player_season_stats" / "parquet" / "player_season_stats_2025.parquet"
    )
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
    py = _without_split_counts(
        pl.read_parquet(
            built_base / "player_season_stats" / "parquet" / "player_season_stats_2025.parquet"
        )
    )
    oracle = oracle_path("player_season_stats", "player_season_stats")
    r = _without_split_counts(pl.read_parquet(oracle))
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
    py = _without_split_counts(
        pl.read_parquet(
            built_base / "player_season_stats" / "parquet" / "player_season_stats_2025.parquet"
        )
    )
    r = _without_split_counts(
        pl.read_parquet(oracle_path("player_season_stats", "player_season_stats"))
    )
    cols = list(r.columns)
    diff = py.select(cols).join(r, on=cols, how="anti", nulls_equal=True)
    assert set(diff.get_column("athlete_id").unique().to_list()) <= set(
        _KNOWN_COLLIDING_ATHLETE_IDS
    )


def _write_tmp(df: pl.DataFrame) -> Path:
    p = Path(tempfile.mkdtemp(prefix="pss_oracle_")) / "filtered.parquet"
    df.write_parquet(p)
    return p
