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
        assert torvik_snapshot.bart_teams(season).height == entry["teams"], name


def test_missing_season_raises_and_pre_torvik_season_is_empty():
    with pytest.raises(FileNotFoundError, match="torvik_snapshot"):
        torvik_snapshot.bart_teams(2099)
    assert torvik_snapshot.bart_teams(2002).height == 0
