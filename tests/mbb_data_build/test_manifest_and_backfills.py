"""The release-asset contracts no season-level oracle happens to exercise in
isolation: manifest endpoints (incl. the two MBB deltas -- officials pointing
at game_rosters, player_season_stats carrying no {season} segment).

MBB delta vs the NBA sibling: there is no ``largest_lead``/``type_abbreviation``
season-level column backfill to test here -- the MBB per-game reshapers always
emit their full fixed column set (see ``reshapers.py`` module docstring). The
only season-level pass is the hoopR#23 player_box dual-team dedupe.
"""

import polars as pl
from mbb_data_build import reshapers
from mbb_data_build.config import REGISTRY
from mbb_data_build.reshapers import SEASON_POSTPROCESS

_RAW = "https://raw.githubusercontent.com/sportsdataverse/hoopR-mbb-raw/main/mbb"

EXPECTED_ENDPOINT = {
    "shots": "derived from espn_mbb pbp",
    "rosters": f"{_RAW}/team_rosters/json/2026/<team_id>.json",
    # MBB delta: flat raw payload -- no {season} segment.
    "player_season_stats": f"{_RAW}/player_season_stats/json/<athlete_id>.json",
    "team_season_stats": f"{_RAW}/team_stats/json/2026/<team_id>.json",
    "standings": f"{_RAW}/standings/json/2026.json",
    "game_rosters": f"{_RAW}/game_rosters/json/<game_id>.json",
    # MBB delta: officials have no dedicated raw dir -- projected from
    # game_rosters.
    "officials": f"{_RAW}/game_rosters/json/<game_id>.json",
    # The crosswalks read a LIVE hoopR/sdv-py surface, not a raw URL, so their
    # source_endpoint is the function name -- verbatim what
    # mbb_1{1,2,3}_*_crosswalk_creation.R write and what the committed
    # mbb/crosswalk/*_in_data_repo.csv rows already carry. All three are now
    # Python-built; team_crosswalk was the last to flip.
    "player_crosswalk": "hoopR::mbb_player_crosswalk()",
    "schedule_crosswalk": "hoopR::mbb_schedule_crosswalk()",
    "team_crosswalk": "hoopR::mbb_team_crosswalk()",
}


def test_exactly_the_manifested_datasets_have_a_manifest():
    manifested = {k for k, v in REGISTRY.items() if v.manifest_endpoint is not None}
    assert manifested == set(EXPECTED_ENDPOINT)


def test_manifest_endpoints_match_the_committed_r_output():
    for dataset, expected in EXPECTED_ENDPOINT.items():
        spec = REGISTRY[dataset]
        assert spec.manifest_endpoint is not None
        assert spec.manifest_endpoint.format(season=2026) == expected, dataset


def test_pbp_team_box_player_box_never_write_a_tree_csv():
    # R's fwrite for these three is commented out in the NBA/MBB scripts.
    for dataset in ("pbp", "team_box", "player_box"):
        assert REGISTRY[dataset].write_tree_csv is False, dataset


def test_no_draft_dataset():
    # MBB (college) has no draft -- 14 datasets, not NBA's 15.
    assert "draft" not in REGISTRY


def test_season_postprocess_is_player_box_dedupe_only():
    # Unlike NBA, MBB's per-game reshapers always emit their full fixed
    # column set -- no season-union column backfill is needed. The ONLY
    # season-level pass is the hoopR#23 dual-team dupe-athlete dedupe.
    assert set(SEASON_POSTPROCESS) == {"player_box"}
    assert SEASON_POSTPROCESS["player_box"] is reshapers.dedupe_player_box_dual_team


def test_dedupe_player_box_dual_team():
    # One game, athlete 1 double-listed on both teams (identical stat line):
    # the starter=True copy wins. Athlete 2 dupe resolved by modal team from
    # game 2. Athlete 3 is a legit single row and must survive untouched.
    df = pl.DataFrame(
        {
            "game_id": [1, 1, 1, 2, 1, 1],
            "athlete_id": [10, 10, 30, 20, 20, 20],
            "team_id": [100, 200, 200, 100, 100, 200],
            "starter": [True, False, False, False, False, False],
        }
    )
    out = reshapers.dedupe_player_box_dual_team(df)
    assert out.height == 4
    kept = {(r["game_id"], r["athlete_id"], r["team_id"]) for r in out.to_dicts()}
    assert kept == {(1, 10, 100), (1, 30, 200), (2, 20, 100), (1, 20, 100)}
