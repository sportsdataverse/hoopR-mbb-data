"""wp_enrich is the pbp asset's only publisher; every assertion here is on the FILE it ships."""

from pathlib import Path

import polars as pl
import pytest
from mbb_data_build import io, publish, wp_enrich
from mbb_data_build.config import REGISTRY

#: release metadata sidecars -- asserted separately, not a data asset
SIDECARS = ("timestamp.", "package_function.")


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


def _write_tree(tmp_path, *, aux=True):
    """The tree the driver leaves behind: pbp plus the two same-run WP inputs."""
    io.write_dataset(_pbp(), REGISTRY["pbp"], 2025, base=tmp_path)
    if aux:
        io.write_dataset(
            pl.DataFrame({"game_id": [1, 2]}), REGISTRY["schedules"], 2025, base=tmp_path
        )
        io.write_dataset(
            pl.DataFrame({"game_id": [1, 1]}), REGISTRY["team_box"], 2025, base=tmp_path
        )


def _stub_gh(monkeypatch):
    calls = []
    monkeypatch.setattr(publish, "_gh", lambda args: calls.append(args))
    monkeypatch.setattr(publish, "_gh_release_exists", lambda tag, repo: True)
    return calls


def test_enrich_rewrites_the_tree_pbp_with_wp_and_publishes_all_formats(tmp_path, monkeypatch):
    _write_tree(tmp_path)
    calls = _stub_gh(monkeypatch)

    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=_compile) is True

    pq = tmp_path / "pbp" / "parquet" / "play_by_play_2025.parquet"
    out = pl.read_parquet(pq)
    assert set(publish.WP_COLS) <= set(out.columns)
    assert out.height == 4 and out["home_win_prob"].null_count() == 0
    assert publish.assert_wp_enriched(pq) == {c: 1.0 for c in publish.WP_COLS}
    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
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
    _write_tree(tmp_path)
    calls = _stub_gh(monkeypatch)

    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=lambda p, s, t: p) is False

    assert calls == []
    tree = pl.read_parquet(tmp_path / "pbp" / "parquet" / "play_by_play_2025.parquet")
    assert not set(publish.WP_COLS) & set(tree.columns)  # tree untouched, still plain


def test_enrich_refuses_a_compile_that_changes_the_row_count(tmp_path, monkeypatch):
    _write_tree(tmp_path)
    calls = _stub_gh(monkeypatch)
    shorter = lambda p, s, t: _compile(p, s, t).head(3)  # noqa: E731
    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=shorter) is False
    assert calls == []


def test_enrich_refuses_a_compile_that_swaps_one_play_for_a_duplicate(tmp_path, monkeypatch):
    # The row count, the schema and the dtypes all survive a drop-plus-duplicate, so only
    # the play-identity multiset catches it.
    _write_tree(tmp_path)
    calls = _stub_gh(monkeypatch)

    def swapper(p, s, t):
        out = _compile(p, s, t)
        return pl.concat([out.head(3), out.head(1)])  # drops (2, 2), duplicates (1, 1)

    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=swapper) is False
    assert calls == []


def test_enrich_with_no_pbp_built_is_not_a_failure(tmp_path, monkeypatch):
    calls = _stub_gh(monkeypatch)
    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=_compile) is True
    assert calls == []


def test_main_exit_code_reflects_a_failed_season(tmp_path, monkeypatch):
    _write_tree(tmp_path)
    _stub_gh(monkeypatch)
    monkeypatch.setattr(wp_enrich, "_default_compile", lambda league: lambda p, s, t: p)
    assert wp_enrich.main(["-s", "2025", "-e", "2025", "--base", str(tmp_path)]) == 1


def test_enrich_refuses_when_a_same_run_input_is_missing(tmp_path, monkeypatch):
    # schedules/team_box are built minutes earlier in the same run; absent means the
    # build failed, and the HFA-only fallback would ship a flat prior as if fresh.
    _write_tree(tmp_path, aux=False)
    calls = _stub_gh(monkeypatch)
    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=_compile) is False
    assert calls == []


def test_publish_refuses_a_pbp_season_with_no_parquet_at_all(tmp_path):
    # a leftover rds/csv must never ship on its own
    with pytest.raises(publish.UnenrichedPbpError, match="no pbp parquet"):
        publish.publish_dataset(REGISTRY["pbp"], 2025, base=tmp_path, dry_run=True)


def test_enrich_refuses_a_compile_that_drops_an_input_column(tmp_path, monkeypatch):
    _write_tree(tmp_path)
    calls = _stub_gh(monkeypatch)
    dropper = lambda p, s, t: _compile(p, s, t).drop("away_score")  # noqa: E731
    assert wp_enrich.enrich_and_publish(2025, base=tmp_path, compile=dropper) is False
    assert calls == []
