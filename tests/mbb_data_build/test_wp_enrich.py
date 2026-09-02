"""wp_enrich is the pbp asset's only publisher; every assertion here is on the FILE it ships."""

from pathlib import Path

import polars as pl
from mbb_data_build import io, publish, wp_enrich
from mbb_data_build.config import REGISTRY


def _pbp():
    return pl.DataFrame(
        {
            "game_id": [1, 1, 2, 2],
            "game_play_number": [1, 2, 1, 2],
            "home_score": [0, 2, 0, 3],
            "away_score": [0, 0, 2, 2],
        }
    )


def _compile(pbp, schedule, team_box):
    return pbp.with_columns(
        pl.lit(0.6).alias("pregame_home_prob"), pl.lit(0.5).alias("home_win_prob")
    )


def _stub_gh(monkeypatch):
    calls = []
    monkeypatch.setattr(publish, "_gh", lambda args: calls.append(args))
    monkeypatch.setattr(publish, "_gh_release_exists", lambda tag, repo: True)
    return calls


def test_enrich_rewrites_the_tree_pbp_with_wp_and_publishes_all_formats(tmp_path, monkeypatch):
    io.write_dataset(_pbp(), REGISTRY["pbp"], 2025, base=tmp_path)
    calls = _stub_gh(monkeypatch)

    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=_compile) is True

    pq = tmp_path / "pbp" / "parquet" / "play_by_play_2025.parquet"
    out = pl.read_parquet(pq)
    assert set(publish.WP_COLS) <= set(out.columns)
    assert out.height == 4 and out["home_win_prob"].null_count() == 0
    assert publish.assert_wp_enriched(pq) == {c: 1.0 for c in publish.WP_COLS}
    uploads = [c for c in calls if c[:2] == ["release", "upload"]]
    assert sorted(Path(c[3]).name for c in uploads) == [
        "play_by_play_2025.csv",
        "play_by_play_2025.parquet",
        "play_by_play_2025.rds",
    ]


def test_enrich_feeds_the_tree_schedule_and_team_box_to_the_compile(tmp_path, monkeypatch):
    io.write_dataset(_pbp(), REGISTRY["pbp"], 2025, base=tmp_path)
    io.write_dataset(
        pl.DataFrame({"game_id": [1, 2, 3]}), REGISTRY["schedules"], 2025, base=tmp_path
    )
    io.write_dataset(pl.DataFrame({"game_id": [1, 1]}), REGISTRY["team_box"], 2025, base=tmp_path)
    _stub_gh(monkeypatch)
    seen = {}

    def compile_(pbp, schedule, team_box):
        seen.update(pbp=pbp.height, schedule=schedule.height, team_box=team_box.height)
        return _compile(pbp, schedule, team_box)

    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=compile_) is True
    assert seen == {"pbp": 4, "schedule": 3, "team_box": 2}


def test_enrich_refuses_to_publish_when_the_compile_adds_no_wp(tmp_path, monkeypatch):
    io.write_dataset(_pbp(), REGISTRY["pbp"], 2025, base=tmp_path)
    calls = _stub_gh(monkeypatch)

    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=lambda p, s, t: p) is False

    assert calls == []
    tree = pl.read_parquet(tmp_path / "pbp" / "parquet" / "play_by_play_2025.parquet")
    assert not set(publish.WP_COLS) & set(tree.columns)  # tree untouched, still plain


def test_enrich_refuses_a_compile_that_changes_the_row_count(tmp_path, monkeypatch):
    io.write_dataset(_pbp(), REGISTRY["pbp"], 2025, base=tmp_path)
    calls = _stub_gh(monkeypatch)
    shorter = lambda p, s, t: _compile(p, s, t).head(3)  # noqa: E731
    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=shorter) is False
    assert calls == []


def test_enrich_with_no_pbp_built_is_not_a_failure(tmp_path, monkeypatch):
    calls = _stub_gh(monkeypatch)
    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=_compile) is True
    assert calls == []


def test_main_exit_code_reflects_a_failed_season(tmp_path, monkeypatch):
    io.write_dataset(_pbp(), REGISTRY["pbp"], 2025, base=tmp_path)
    _stub_gh(monkeypatch)
    monkeypatch.setattr(wp_enrich, "_default_compile", lambda league: lambda p, s, t: p)
    assert wp_enrich.main(["-s", "2025", "-e", "2025", "--base", str(tmp_path)]) == 1
