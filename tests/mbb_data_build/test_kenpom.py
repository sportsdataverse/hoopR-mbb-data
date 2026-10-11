"""KenPom id legs of the schedule + player crosswalks (``mbb_data_build.kenpom``)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
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
    monkeypatch.setattr(kenpom, "published", lambda *a: None)  # nothing published to carry
    players = pl.DataFrame({"espn_team_id": [1], "player_name": ["x"], "yahoo_player_name": [None]})
    out = kenpom.add_kp_player_id(players, 2025, tmp_path)
    assert out["kp_player_id"].dtype == pl.Utf8 and out["kp_player_id"].null_count() == 1


# --- carry-forward + KenPom-only fill, on the committed 2026 crosswalks (real shape) ---

REPO_XW = Path(__file__).resolve().parents[2] / "mbb" / "crosswalk" / "parquet"


def _real(stem):
    f = REPO_XW / f"{stem}_2026.parquet"
    if not f.exists():
        pytest.skip(f"{f.name} not in this checkout")
    return pl.read_parquet(f)


def _tree(tmp_path, *frames):
    base = tmp_path / "mbb"
    (base / "crosswalk/parquet").mkdir(parents=True)
    for stem, df in frames:
        df.write_parquet(base / f"crosswalk/parquet/{stem}_2026.parquet")
    return base


def test_missing_checkout_carries_published_game_ids(tmp_path, monkeypatch):
    # The GitHub build: no hoopR-kp-data, a rebuilt frame in a new row order with one new game.
    monkeypatch.setenv("HOOPR_KP_DATA_ROOT", str(tmp_path / "absent"))
    pub = _real("mbb_schedule_crosswalk")
    base = _tree(tmp_path, ("mbb_schedule_crosswalk", pub))
    new_game = pub.head(1).with_columns(
        pl.lit("999999999").alias("espn_game_id"), pl.lit(None, dtype=pl.Utf8).alias("kp_game_id")
    )
    rebuilt = pl.concat([pub.reverse(), new_game]).with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("kp_game_id")
    )
    out = kenpom.add_kp_game_id(rebuilt, 2026, base)
    assert out.schema == rebuilt.schema and out.height == rebuilt.height
    assert out["espn_game_id"].to_list() == rebuilt["espn_game_id"].to_list()
    assert out.head(pub.height).equals(pub.reverse())
    assert out["kp_game_id"][-1] is None


def test_missing_checkout_carries_published_player_ids(tmp_path, monkeypatch):
    monkeypatch.setenv("HOOPR_KP_DATA_ROOT", str(tmp_path / "absent"))
    pub = _real("mbb_player_crosswalk")
    base = _tree(tmp_path, ("mbb_player_crosswalk", pub))
    out = kenpom.add_kp_player_id(pub.drop("kp_player_id").sample(fraction=1.0, seed=7), 2026, base)
    assert out.columns == pub.columns
    joined = out.join(pub, on=["espn_team_id", "espn_athlete_id"], suffix="_pub")
    assert joined.height == pub.height
    assert (joined["kp_player_id"].eq_missing(joined["kp_player_id_pub"])).all()


def test_carry_forward_keeps_a_fresh_id(tmp_path):
    pub = _real("mbb_schedule_crosswalk").head(3)
    fresh = pub.with_columns(pl.Series("kp_game_id", ["1", None, None], dtype=pl.Utf8))
    old = pub.with_columns(pl.Series("kp_game_id", ["9", "8", None], dtype=pl.Utf8))
    out = kenpom.carry_forward(fresh, "kp_game_id", 2026, tmp_path, prev=old)
    assert out["kp_game_id"].to_list() == ["1", "8", None]


def test_kp_fill_writes_only_on_change(tmp_path, monkeypatch):
    from mbb_data_build import kp_fill

    # KenPom capture whose one game matches the first published game; that row was null.
    pub = _real("mbb_schedule_crosswalk")
    g = pub.row(0, named=True)
    kp = tmp_path / "kp" / "data/team_schedules/csv"
    kp.mkdir(parents=True)
    pl.DataFrame(
        {
            "Team": ["A", "B"],
            "Opponent": ["B", "A"],
            "GameDate": [g["game_date"].strftime("%Y%m%d")] * 2,
            "GameId": ["4242", "4242"],
        }
    ).write_csv(kp / "team_schedules_2026.csv")
    monkeypatch.setenv("HOOPR_KP_DATA_ROOT", str(tmp_path / "kp"))
    teams = pl.DataFrame(
        {
            "kp_team": ["A", "B"],
            "espn_team_id": pl.Series(
                [g["home_espn_team_id"], g["away_espn_team_id"]], dtype=pl.Int32
            ),
        }
    )
    blank = pub.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(None)
        .otherwise(pl.col("kp_game_id"))
        .alias("kp_game_id")
    )
    base = _tree(tmp_path, ("mbb_schedule_crosswalk", blank), ("mbb_team_crosswalk", teams))
    out = kp_fill.fill("schedule_crosswalk", 2026, base=base)
    assert out["kp_game_id"][0] == "4242"
    assert out.slice(1).equals(pub.slice(1))  # every other published id carried
    written = pl.read_parquet(base / "crosswalk/parquet/mbb_schedule_crosswalk_2026.parquet")
    assert written.equals(out)
    # A second pass finds nothing new and writes nothing.
    f = base / "crosswalk/parquet/mbb_schedule_crosswalk_2026.parquet"
    mtime = f.stat().st_mtime_ns
    assert kp_fill.fill("schedule_crosswalk", 2026, base=base).equals(out)
    assert f.stat().st_mtime_ns == mtime


def test_kp_fill_preseason_is_a_no_op(tmp_path, monkeypatch):
    from mbb_data_build import kp_fill

    monkeypatch.setenv("HOOPR_KP_DATA_ROOT", str(tmp_path / "kp"))
    monkeypatch.setattr(kenpom, "published", lambda *a: None)
    assert kp_fill.fill("player_crosswalk", 2027, base=tmp_path) is None
