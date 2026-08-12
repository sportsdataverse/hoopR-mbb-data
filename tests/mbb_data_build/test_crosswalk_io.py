"""The crosswalk output contract: shared ``mbb/crosswalk/`` dir, no tree csv,
bespoke rds type strings, and a manifest that UPSERTS instead of appending.

R's ``mbb_1{1,2,3}_*_crosswalk_creation.R`` hard-code one shared directory.
One row per season is the manifest contract; if the Python producer appended
like the per-game datasets do, a daily re-run would grow a duplicate row per
season -- which is exactly how the committed
``mbb_player_crosswalk_in_data_repo.csv`` ended up with nine 2026 rows.

Only ``player_crosswalk`` is Python-built (team joins the paid KenPom feed;
schedule has no committed golden), so only it carries build metadata -- but
all three share the directory, which the first tests assert for all of them.
"""

import polars as pl
import pytest
from mbb_data_build import build, io, publish, reshapers
from mbb_data_build.config import REGISTRY

CROSSWALKS = ("team_crosswalk", "schedule_crosswalk", "player_crosswalk")


def _frame() -> pl.DataFrame:
    return pl.DataFrame({"season": [2026, 2026], "espn_athlete_id": ["1", "2"]})


def test_all_three_crosswalks_share_one_directory(tmp_path):
    for ds in CROSSWALKS:
        assert io.dataset_dir(REGISTRY[ds], tmp_path) == tmp_path / "crosswalk"


def test_non_crosswalk_datasets_still_live_under_their_own_name(tmp_path):
    assert io.dataset_dir(REGISTRY["pbp"], tmp_path) == tmp_path / "pbp"


def test_manifest_file_name_still_carries_the_dataset(tmp_path):
    assert io.manifest_path(REGISTRY["player_crosswalk"], tmp_path) == (
        tmp_path / "crosswalk" / "mbb_player_crosswalk_in_data_repo.csv"
    )


def test_crosswalks_write_no_tree_csv_the_tree_csv_is_the_manifest():
    for ds in CROSSWALKS:
        assert REGISTRY[ds].write_tree_csv is False, ds


def test_write_lands_under_crosswalk_not_under_the_dataset_name(tmp_path):
    spec = REGISTRY["player_crosswalk"]
    paths = io.write_dataset(_frame(), spec, 2026, base=tmp_path)
    assert (tmp_path / "crosswalk" / "parquet" / "mbb_player_crosswalk_2026.parquet").exists()
    assert (tmp_path / "crosswalk" / "rds" / "mbb_player_crosswalk_2026.rds").exists()
    assert not (tmp_path / "player_crosswalk").exists()
    assert not any(p.suffix == ".csv" and p.parent.name == "csv" for p in paths)


def test_rerunning_a_season_upserts_the_manifest_row(tmp_path):
    spec = REGISTRY["player_crosswalk"]
    for _ in range(3):
        io.write_dataset(_frame(), spec, 2026, base=tmp_path)
    io.write_dataset(_frame(), spec, 2025, base=tmp_path)
    m = pl.read_csv(io.manifest_path(spec, tmp_path))
    assert m["season"].to_list() == [2025, 2026]  # sorted, one row per season
    assert m["source_endpoint"].unique().to_list() == ["hoopR::mbb_player_crosswalk()"]


def test_per_game_datasets_still_append_their_manifest_log(tmp_path):
    spec = REGISTRY["rosters"]
    assert spec.manifest_upsert is False
    for _ in range(3):
        io._append_manifest(spec, 2026, 5, tmp_path)
    assert pl.read_csv(io.manifest_path(spec, tmp_path)).height == 3


def test_publish_finds_the_crosswalk_files_under_the_shared_dir(tmp_path):
    spec = REGISTRY["player_crosswalk"]
    io.write_dataset(_frame(), spec, 2026, base=tmp_path)
    names = [p.name for p in publish._dataset_files(spec, 2026, tmp_path)]
    assert "mbb_player_crosswalk_2026.parquet" in names
    assert "mbb_player_crosswalk_2026.rds" in names
    # No tree csv, but the release contract still ships one (built from parquet).
    assert "mbb_player_crosswalk_2026.csv" in names
    assert "mbb_player_crosswalk_in_data_repo.csv" in names


def test_rds_carries_the_bespoke_crosswalk_type_not_the_generic_template():
    spec = REGISTRY["player_crosswalk"]
    # hoopR/R/mbb_crosswalk.R make_hoopR_data() + the R script's sportsdataverse_type
    assert spec.rds_type == "MBB player crosswalk (ESPN / Fox)"
    assert spec.sdv_type == "player crosswalk data"


def test_player_crosswalk_never_resolves_a_raw_root(monkeypatch, tmp_path):
    """NO_RAW_INPUT: build_season must not touch ingest.raw_root for it --
    the crosswalk reads live ESPN/Fox and would otherwise fail on a machine
    with no hoopR-mbb-raw checkout, for an input it never opens."""
    assert "player_crosswalk" in reshapers.NO_RAW_INPUT

    def _boom(*a, **k):
        raise AssertionError("raw_root resolved for a live-source crosswalk")

    monkeypatch.setattr(build.ingest, "raw_root", _boom)
    monkeypatch.setitem(reshapers.SEASON_BUILDERS, "player_crosswalk", lambda season, **k: _frame())
    out = build.build_season("player_crosswalk", 2026, base=tmp_path)
    assert out.height == 2


def test_team_and_schedule_crosswalks_are_still_R_built(tmp_path):
    """They must NOT have acquired a Python builder by accident -- team joins
    the paid KenPom feed, schedule has no committed golden to gate against."""
    for ds in ("team_crosswalk", "schedule_crosswalk"):
        assert ds not in reshapers.SEASON_BUILDERS, ds
        assert ds not in reshapers.NO_RAW_INPUT, ds
        with pytest.raises(NotImplementedError):
            build.build_season(ds, 2026, base=tmp_path)
