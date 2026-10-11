"""KenPom id legs of the schedule + player crosswalks (``mbb_data_build.kenpom``)."""

from __future__ import annotations

from datetime import date

import polars as pl
from mbb_data_build import kenpom


def _setup(tmp_path, monkeypatch):
    kp = tmp_path / "kp"
    (kp / "data/team_schedules/csv").mkdir(parents=True)
    (kp / "data/players/csv").mkdir(parents=True)
    pl.DataFrame(
        {
            "Team": ["Duke", "North Carolina", "Duke"],
            "Opponent": ["North Carolina", "Duke", "McMurry"],
            "GameDate": ["20250308", "20250308", "20241104"],
            "GameId": ["1201", "1201", "3"],
        }
    ).write_csv(kp / "data/team_schedules/csv/team_schedules_2025.csv")
    pl.DataFrame(
        {
            "Team": ["Duke", "Duke", "Duke"],
            "Player": ["Cooper Flagg", "Tyrese Proctor\n   ", "Tyrese Proctor"],
            "Number": ["2", "5", "11"],
            "PlayerId": ["70001", "70002", "70003"],
        }
    ).write_csv(kp / "data/players/csv/players_2025.csv")
    monkeypatch.setenv("HOOPR_KP_DATA_ROOT", str(kp))
    base = tmp_path / "mbb"
    (base / "crosswalk/parquet").mkdir(parents=True)
    pl.DataFrame(
        {
            "kp_team": ["Duke", "North Carolina"],
            "espn_team_id": pl.Series([150, 153], dtype=pl.Int32),
        }
    ).write_parquet(base / "crosswalk/parquet/mbb_team_crosswalk_2025.parquet")
    return base


def test_kp_game_id_matches_unordered_team_pair_on_date(tmp_path, monkeypatch):
    base = _setup(tmp_path, monkeypatch)
    sched = pl.DataFrame(
        {
            "season": pl.Series([2025, 2025, 2025], dtype=pl.Int32),
            "game_date": [date(2025, 3, 8), date(2025, 3, 9), date(2024, 11, 4)],
            "home_espn_team_id": pl.Series([150, 150, 150], dtype=pl.Int32),
            "away_espn_team_id": pl.Series([153, 153, 999999], dtype=pl.Int32),
            "kp_game_id": pl.Series([None, None, None], dtype=pl.Utf8),
        }
    )
    out = kenpom.add_kp_game_id(sched, 2025, base)
    assert out.schema == sched.schema
    # Row 3: non-D-I opponent, matched on (date, the one D-I team).
    assert out["kp_game_id"].to_list() == ["1201", None, "3"]


def test_kp_player_id_name_then_jersey_tiebreak(tmp_path, monkeypatch):
    base = _setup(tmp_path, monkeypatch)
    players = pl.DataFrame(
        {
            "espn_team_id": pl.Series([150, 150, 150], dtype=pl.Int32),
            "player_name": ["cooper flagg", "tyrese proctor", "nobody"],
            "espn_jersey": ["2", "5", "9"],
            "yahoo_player_name": [None, None, None],
            "match_method": ["exact_name"] * 3,
        }
    )
    out = kenpom.add_kp_player_id(players, 2025, base)
    assert out.columns.index("kp_player_id") == out.columns.index("yahoo_player_name") + 1
    assert out["kp_player_id"].to_list() == ["70001", "70002", None]


def test_missing_checkout_leaves_ids_null(tmp_path, monkeypatch):
    monkeypatch.setenv("HOOPR_KP_DATA_ROOT", str(tmp_path / "absent"))
    players = pl.DataFrame({"espn_team_id": [1], "player_name": ["x"], "yahoo_player_name": [None]})
    out = kenpom.add_kp_player_id(players, 2025, tmp_path)
    assert out["kp_player_id"].dtype == pl.Utf8 and out["kp_player_id"].null_count() == 1
