"""Hermetic tests for the mbb model-publish builders.

The sdv-py compute seams are stubbed, so these assert *orchestration* --
season ordering, the empty-frame refusal, the floor, the card sidecars, and
per-file upload -- not the ratings/BPM math (gated in sdv-py's oracle suites).
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from mbb_model_publish.artifacts import upload_artifacts
from mbb_model_publish.builders import (
    MIN_SEASON,
    build_player_value,
    build_ratings,
    write_player_value_card,
    write_ratings_card,
)
from mbb_model_publish.cli import _seasons, main


def _fake_ratings(season: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [season, season],
            "team_id": ["52", "150"],
            "adj_o": [118.2, 112.4],
            "adj_d": [93.5, 96.1],
            "adj_em": [24.7, 16.3],
            "rank": [1, 2],
        }
    )


def _fake_bpm(season: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "player_id": ["4433137", "4592971"],
            "season": [season, season],
            "team_id": ["52", "150"],
            "box_bpm": [8.4, 6.1],
        }
    )


def test_build_ratings_writes_one_parquet_per_season_in_order(tmp_path):
    results = build_ratings([2024, 2025], tmp_path, compute=_fake_ratings)

    assert [r["season"] for r in results] == [2024, 2025]
    for season in (2024, 2025):
        path = tmp_path / f"mbb_ratings_{season}.parquet"
        assert path.exists()
        assert pl.read_parquet(path)["season"].unique().to_list() == [season]


def test_build_player_value_writes_per_season(tmp_path):
    results = build_player_value([2025], tmp_path, compute=_fake_bpm)

    assert [r["rows"] for r in results] == [2]
    assert (tmp_path / "mbb_player_value_2025.parquet").exists()


def test_builders_refuse_an_empty_season(tmp_path):
    empty = pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8})

    with pytest.raises(ValueError, match="0 rows"):
        build_ratings([2025], tmp_path, compute=lambda s: empty)
    with pytest.raises(ValueError, match="0 rows"):
        build_player_value([2025], tmp_path, compute=lambda s: empty)


def test_builders_reject_seasons_below_the_floor(tmp_path):
    with pytest.raises(ValueError, match=str(MIN_SEASON)):
        build_ratings([MIN_SEASON - 1], tmp_path, compute=_fake_ratings)
    with pytest.raises(ValueError, match=str(MIN_SEASON)):
        build_player_value([MIN_SEASON - 1], tmp_path, compute=_fake_bpm)


def test_cards_carry_tag_and_seasons(tmp_path):
    r = build_ratings([2025], tmp_path, compute=_fake_ratings)
    card = json.loads(write_ratings_card(r, tmp_path).read_text(encoding="utf-8"))
    assert card["tag"] == "mbb_ratings"
    assert card["rows_by_season"] == {"2025": 2}

    v = build_player_value([2025], tmp_path, compute=_fake_bpm)
    card = json.loads(write_player_value_card(v, tmp_path).read_text(encoding="utf-8"))
    assert card["tag"] == "mbb_player_value"
    assert card["seasons"] == [2025]


def test_upload_pattern_selects_parquet_and_card(tmp_path):
    (tmp_path / "mbb_ratings_2025.parquet").write_bytes(b"x")
    (tmp_path / "mbb_ratings_card.json").write_text("{}")
    (tmp_path / "unrelated.txt").write_text("no")

    calls: list = []
    res = upload_artifacts(
        tmp_path,
        "mbb_ratings",
        "sportsdataverse/sportsdataverse-data",
        pattern="mbb_ratings_*.*",
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )

    names = sorted(p.rsplit("\\", 1)[-1].rsplit("/", 1)[-1] for p in res["files"])
    assert names == ["mbb_ratings_2025.parquet", "mbb_ratings_card.json"]
    assert res["uploaded"] == 2
    assert all("--clobber" in c for c in calls)


def test_seasons_parses_range_and_single():
    assert _seasons("2025") == [2025]
    assert _seasons("2002:2005") == [2002, 2003, 2004, 2005]


def test_cli_build_only_writes_files_and_skips_upload(tmp_path, monkeypatch):
    import mbb_model_publish.cli as cli

    monkeypatch.setattr(
        cli,
        "build_ratings",
        lambda seasons, out, **kw: build_ratings(seasons, out, compute=_fake_ratings),
    )
    monkeypatch.setattr(
        cli,
        "upload_artifacts",
        lambda *a, **k: pytest.fail("--build-only must not upload"),
    )

    rc = main(["ratings", "--seasons", "2025", "--out", str(tmp_path), "--build-only"])

    assert rc == 0
    assert (tmp_path / "mbb_ratings_2025.parquet").exists()
    assert (tmp_path / "mbb_ratings_card.json").exists()
