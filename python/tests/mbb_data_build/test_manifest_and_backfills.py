"""The release-asset contracts no season-level oracle happens to exercise in
isolation: manifest endpoints (incl. the two MBB deltas -- officials pointing
at game_rosters, player_season_stats carrying no {season} segment).

MBB delta vs the NBA sibling: there is no ``largest_lead``/``type_abbreviation``
season-level backfill to test here -- ``reshapers.SEASON_POSTPROCESS`` is
empty because the MBB per-game reshapers always emit their full fixed column
set (see ``reshapers.py`` module docstring).
"""

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


def test_season_postprocess_is_empty():
    # Unlike NBA, MBB's per-game reshapers always emit their full fixed
    # column set -- no season-union backfill is needed.
    assert SEASON_POSTPROCESS == {}
