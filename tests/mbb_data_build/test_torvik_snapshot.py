"""The team crosswalk joins Torvik from the committed ``torvik/`` snapshot.

barttorvik.com 403s every datacenter IP, so a build that fetched live would
fail on CI and the droplet. These pin that the builder hands sdv-py the
snapshot (so sdv-py never fetches) and that every committed file parses.
"""

import json

import pytest
import sportsdataverse.mbb as sdv_mbb
from mbb_data_build import reshapers, torvik_snapshot


def test_team_crosswalk_passes_the_snapshot_as_bart(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.setattr(sdv_mbb, "mbb_team_crosswalk", lambda **k: seen.update(k) or "out")
    assert reshapers.team_crosswalk_builder(2026, raw_root=None, base=tmp_path) == "out"
    assert seen["bart"].equals(torvik_snapshot.bart_teams(2026))
    assert seen["bart"].height > 300


def test_every_manifest_entry_is_committed_and_parses():
    manifest = json.loads((torvik_snapshot.ROOT / "manifest.json").read_text())
    assert manifest, "torvik/manifest.json is empty"
    for name, entry in manifest.items():
        season = int(name.split("_")[0])
        if name.endswith("_super_sked.json"):
            assert torvik_snapshot.bart_games(season).height == entry["games"], name
        else:
            assert torvik_snapshot.bart_teams(season).height == entry["teams"], name


def test_missing_season_raises_and_pre_torvik_season_is_empty():
    with pytest.raises(FileNotFoundError, match="torvik_snapshot"):
        torvik_snapshot.bart_teams(2099)
    assert torvik_snapshot.bart_teams(2002).height == 0


class _Stop(Exception):
    pass


def _capture(seen):
    def fake(**k):
        seen.update(k)
        raise _Stop  # stop before the kenpom / alt_fill / ncaa steps, which need checkouts

    return fake


def test_schedule_crosswalk_passes_the_committed_schedule_and_teams(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.setattr(sdv_mbb, "mbb_schedule_crosswalk", _capture(seen))
    with pytest.raises(_Stop):
        reshapers.schedule_crosswalk_builder(2027, raw_root=None, base=tmp_path)
    assert seen["bart"].equals(torvik_snapshot.bart_teams(2027))
    assert seen["bart_games"].equals(torvik_snapshot.bart_games(2027))
    assert seen["bart_games"].height > 5000
    assert seen["bart_games"]["game_date"].dt.year().min() == 2026  # the 2026-27 season


@pytest.mark.parametrize("blank", [None, ""])  # Torvik ships "" for an unplayed game
@pytest.mark.parametrize("season", [2025, 2026])
def test_bart_winner_from_espn_matches_torvik(season, blank):
    # Real committed tree: blank every Torvik winner, rebuild it from ESPN, and compare.
    import polars as pl

    base = torvik_snapshot.ROOT.parent / "mbb"
    xwalk = pl.read_parquet(
        base / "crosswalk" / "parquet" / f"mbb_schedule_crosswalk_{season}.parquet"
    )
    got = torvik_snapshot.fill_bart_winner(
        xwalk.with_columns(pl.lit(blank, pl.Utf8).alias("bart_winner")), season, base
    )
    assert got.columns == xwalk.columns and got.height == xwalk.height

    def won(s):
        return s.is_not_null() & (s != "")

    both = won(got["bart_winner"]) & won(xwalk["bart_winner"])
    assert (got.filter(both)["bart_winner"] == xwalk.filter(both)["bart_winner"]).all()
    # Only a game in the season's ESPN schedule has a result to read: bart_only rows (no ESPN
    # game) and games the committed schedule lacks (300 in WBB 2025) stay null.
    sched = pl.read_parquet(
        base / "schedules" / "parquet" / f"mbb_schedule_{season}.parquet", columns=["game_id"]
    )
    in_sched = xwalk["espn_game_id"].is_in(sched["game_id"].cast(pl.Int64).cast(pl.Utf8).implode())
    fillable = won(xwalk["bart_winner"]) & in_sched
    assert both.sum() >= 0.99 * fillable.sum()


def test_bart_winner_never_overwrites_torvik(tmp_path):
    import polars as pl

    base = torvik_snapshot.ROOT.parent / "mbb"
    xwalk = pl.read_parquet(base / "crosswalk" / "parquet" / "mbb_schedule_crosswalk_2026.parquet")
    planted = xwalk.with_columns(
        pl.when(pl.col("bart_winner").is_not_null()).then(pl.lit("KEEP")).alias("bart_winner")
    )
    got = torvik_snapshot.fill_bart_winner(planted, 2026, base)
    assert got.filter(planted["bart_winner"] == "KEEP")["bart_winner"].eq("KEEP").all()
    assert torvik_snapshot.fill_bart_winner(planted, 2026, tmp_path).equals(
        planted
    )  # no tree: unchanged


def test_schedule_crosswalk_without_a_snapshot_still_fetches_live(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.setattr(sdv_mbb, "mbb_schedule_crosswalk", _capture(seen))
    with pytest.raises(_Stop):
        reshapers.schedule_crosswalk_builder(2010, raw_root=None, base=tmp_path)
    assert "bart" not in seen and "bart_games" not in seen
