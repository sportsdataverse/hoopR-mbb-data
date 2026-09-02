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
    ADJ_EM_SD_BAND,
    MIN_GATED_TEAMS,
    MIN_SEASON,
    QUALIFIED_MIN_MINUTES,
    RATINGS_LEVEL_BANDS,
    assert_ratings_level,
    build_player_value,
    build_ratings,
    write_player_value_card,
    write_player_value_coefficients,
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
            "adj_tempo": [68.0, 70.5],
            "games": [31, 30],
            "rank": [1, 2],
        }
    )


def _fake_bpm(season: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "player_id": ["4433137", "4592971"],
            "season": [season, season],
            "team_id": ["52", "150"],
            "min": [812.0, 140.0],
            "box_bpm": [8.4, 6.1],
        }
    )


def _league_like_ratings(
    n: int = MIN_GATED_TEAMS, *, em_scale: float = 1.0, games: int = 30
) -> pl.DataFrame:
    """A plausible D1-core season: adj_em symmetric around ~1 with sd ~14, per-100 levels ~105."""
    import numpy as np

    rng = np.random.default_rng(7)
    em = rng.normal(1.0, 14.0, n) * em_scale
    adj_o = 105.0 + em / 2
    return pl.DataFrame(
        {
            "season": [2025] * n,
            "team_id": [str(i) for i in range(n)],
            "adj_o": adj_o,
            "adj_d": adj_o - em,
            "adj_em": em,
            "adj_tempo": rng.normal(68.0, 3.0, n),
            "games": [games] * n,
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


# ---- player_value: the additive `qualified` flag ---------------------------------


def test_player_value_appends_qualified_without_dropping_rows(tmp_path):
    build_player_value([2025], tmp_path, compute=_fake_bpm)
    out = pl.read_parquet(tmp_path / "mbb_player_value_2025.parquet")
    assert out.height == 2  # never a filter
    assert set(_fake_bpm(2025).columns) <= set(out.columns)  # every published column preserved
    assert out["qualified"].to_list() == [
        812.0 >= QUALIFIED_MIN_MINUTES,
        140.0 >= QUALIFIED_MIN_MINUTES,
    ]
    assert out["qualified"].to_list() == [True, False]


def test_player_value_refuses_a_frame_without_minutes(tmp_path):
    with pytest.raises(ValueError, match="'min'"):
        build_player_value([2025], tmp_path, compute=lambda s: _fake_bpm(s).drop("min"))


# ---- ratings: the absolute level-band gate beside the rank gates -----------------


def test_ratings_level_gate_passes_a_league_shaped_season_and_records_it(tmp_path):
    results = build_ratings([2025], tmp_path, compute=lambda s: _league_like_ratings())
    gate = results[0]["gate"]
    assert gate["applied"] is True and gate["teams"] == MIN_GATED_TEAMS
    lo, hi = RATINGS_LEVEL_BANDS["adj_o"]
    assert lo <= gate["mean_adj_o"] <= hi
    assert ADJ_EM_SD_BAND[0] <= gate["sd_adj_em"] <= ADJ_EM_SD_BAND[1]
    card = json.loads(write_ratings_card(results, tmp_path).read_text(encoding="utf-8"))
    assert card["gates_by_season"]["2025"]["applied"] is True


def test_ratings_level_gate_refuses_a_rescaled_season_the_rank_gate_would_pass():
    # x100 keeps every rank identical (Spearman = 1.0 vs the true ratings) -- the
    # scale-blind failure -- and must be refused on level alone.
    with pytest.raises(ValueError, match="level band violated"):
        assert_ratings_level(_league_like_ratings(em_scale=100.0), 2025)
    # per-game instead of per-100 (everything / 1.5): ranks intact, levels off.
    perg = _league_like_ratings().with_columns(
        pl.col("adj_o") / 1.5, pl.col("adj_d") / 1.5, pl.col("adj_em") / 1.5
    )
    with pytest.raises(ValueError, match="level band violated"):
        assert_ratings_level(perg, 2025)


def test_ratings_level_gate_refuses_a_nan_fixed_point():
    # The published mbb_ratings_2011 asset is 604/604 NaN; NaN is not null in polars.
    nan = _league_like_ratings().with_columns(pl.lit(float("nan")).alias("adj_em"))
    with pytest.raises(ValueError, match="non-finite adj_em"):
        assert_ratings_level(nan, 2011)


def test_ratings_level_gate_refuses_infinite_ratings():
    # is_nan() does NOT match +/-inf in polars: on [1.0, nan, inf, -inf, null] the
    # old is_null()|is_nan() predicate caught 2 of the 4 bad values (polars 1.42.1),
    # so an infinite fixed point would have published. Both signs must be refused.
    for value in (float("inf"), float("-inf")):
        bad = _league_like_ratings().with_columns(pl.lit(value).alias("adj_em"))
        with pytest.raises(ValueError, match="non-finite adj_em"):
            assert_ratings_level(bad, 2011)


def test_ratings_level_gate_does_not_apply_to_a_season_too_young_to_have_a_level():
    rec = assert_ratings_level(_league_like_ratings(n=40), 2026)  # early November
    assert rec == {"applied": False, "teams": 40}
    rec = assert_ratings_level(_league_like_ratings(games=3), 2026)  # nobody has 10 games yet
    assert rec["applied"] is False and rec["teams"] == 0


# ---- player_value: the shipped coefficient vector --------------------------------


def test_player_value_coefficients_sidecar_ships_the_artifact_with_provenance(tmp_path):
    art = {
        "league": "mens",
        "feature_cols": ["usage", "ts_pct"],
        "obpm_coef": [-2.6, 1.9, 0.3],
        "dbpm_coef": [1.5, -0.1, 0.2],
        "feature_mean": {"usage": 0.2, "ts_pct": 0.5},
        "feature_sd": {"usage": 0.05, "ts_pct": 0.06},
        "min_minutes": 150.0,
        "train_seasons": [2025, 2026],
    }
    path = write_player_value_coefficients(tmp_path, load=lambda: art)
    assert (
        path.name == "mbb_player_value_coefficients.json"
    )  # matches the mbb_player_value_*.* upload glob
    out = json.loads(path.read_text(encoding="utf-8"))
    assert out["feature_cols"] == art["feature_cols"]
    assert len(out["obpm_coef"]) == len(out["feature_cols"]) + 1  # intercept + slopes
    assert out["artifact_sha256"] and out["written"] and "sportsdataverse_version" in out
