from pathlib import Path

import polars as pl
import pytest
from mbb_data_build import io, publish
from mbb_data_build.config import REGISTRY

#: release metadata sidecars -- asserted separately, not a data asset
SIDECARS = ("timestamp.", "package_function.")


def test_publish_uploads_each_file_with_clobber(tmp_path):
    spec = REGISTRY["schedules"]  # write_tree_csv=True, not manifested
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    res = publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,  # release already exists
    )
    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assets = sorted(Path(c[3]).name for c in uploads)  # gh release upload <tag> <path>
    assert assets == ["mbb_schedule_2025.csv", "mbb_schedule_2025.parquet", "mbb_schedule_2025.rds"]
    assert all("--clobber" in c for c in uploads)
    assert res["tag"] == spec.tag


def test_publish_generates_csv_on_the_fly_for_write_tree_csv_false(tmp_path):
    # team_box has no committed tree csv -- publish must still ship a plain
    # .csv release asset, built from the parquet at publish time.
    spec = REGISTRY["team_box"]
    io.write_dataset(pl.DataFrame({"game_id": [1], "team_id": [2]}), spec, 2025, base=tmp_path)
    assert not (tmp_path / "team_box" / "csv").exists()
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )
    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assets = sorted(Path(c[3]).name for c in uploads)
    assert assets == ["team_box_2025.csv", "team_box_2025.parquet", "team_box_2025.rds"]
    # the on-the-fly csv actually carries the data (not an empty placeholder).
    csv_path = next(Path(c[3]) for c in uploads if Path(c[3]).name == "team_box_2025.csv")
    assert pl.read_csv(csv_path).height == 1


def test_publish_uploads_manifest_for_manifested_datasets(tmp_path):
    spec = REGISTRY["standings"]
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )
    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assets = sorted(Path(c[3]).name for c in uploads)
    assert assets == [
        "mbb_standings_in_data_repo.csv",
        "standings_2025.csv",
        "standings_2025.parquet",
        "standings_2025.rds",
    ]


def test_published_manifest_collapses_the_log_to_the_latest_run_per_season(tmp_path):
    spec = REGISTRY["standings"]
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2024, base=tmp_path)
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    io.write_dataset(pl.DataFrame({"team_id": [1, 2, 3]}), spec, 2025, base=tmp_path)  # rerun

    asset = publish._manifest_asset(spec, tmp_path)
    m = pl.read_csv(asset)
    assert m["season"].to_list() == [2024, 2025]
    assert m["row_count"].to_list() == [1, 3]


def test_publish_creates_release_when_missing(tmp_path):
    spec = REGISTRY["schedules"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: False,
    )
    assert any(c[:2] == ["release", "create"] for c in calls)


# ---- the pbp WP contract: the asset ships enriched or not at all ----------------


def _pbp_frame(**wp):
    base = {
        "game_id": [1, 1],
        "game_play_number": [1, 2],
        "home_score": [0, 2],
        "away_score": [0, 0],
    }
    return pl.DataFrame({**base, **wp})


def test_publish_refuses_a_pbp_parquet_without_wp_columns(tmp_path):
    spec = REGISTRY["pbp"]
    io.write_dataset(_pbp_frame(), spec, 2025, base=tmp_path)
    calls = []
    with pytest.raises(publish.UnenrichedPbpError, match="missing WP columns"):
        publish.publish_dataset(
            spec,
            2025,
            base=tmp_path,
            runner=lambda a: calls.append(a),
            exists_check=lambda t, r: True,
        )
    assert calls == []  # nothing reached gh -- not even the release-exists probe


def test_publish_refuses_a_pbp_parquet_whose_wp_is_not_finite(tmp_path):
    spec = REGISTRY["pbp"]
    io.write_dataset(
        _pbp_frame(pregame_home_prob=[0.6, 0.6], home_win_prob=[None, float("nan")]),
        spec,
        2025,
        base=tmp_path,
    )
    with pytest.raises(publish.UnenrichedPbpError, match="finite-rate floor"):
        publish.publish_dataset(spec, 2025, base=tmp_path, dry_run=True)  # dry runs are refused too


def test_publish_refuses_string_typed_wp_columns(tmp_path):
    """A numeric STRING casts cleanly, so the finite-rate floor alone let it through."""
    spec = REGISTRY["pbp"]
    io.write_dataset(
        _pbp_frame(pregame_home_prob=[0.6, 0.6], home_win_prob=["0.62", "0.55"]),
        spec,
        2025,
        base=tmp_path,
    )
    pq = tmp_path / "pbp" / "parquet" / "play_by_play_2025.parquet"
    # the old cast-based check would have scored these as 100% finite
    assert pl.read_parquet(pq)["home_win_prob"].cast(pl.Float64, strict=False).is_finite().all()
    with pytest.raises(publish.UnenrichedPbpError, match="not float-typed"):
        publish.publish_dataset(spec, 2025, base=tmp_path, dry_run=True)


def test_publish_accepts_an_enriched_pbp(tmp_path):
    spec = REGISTRY["pbp"]
    io.write_dataset(
        _pbp_frame(pregame_home_prob=[0.6, 0.6], home_win_prob=[0.55, 0.7]),
        spec,
        2025,
        base=tmp_path,
    )
    calls = []
    publish.publish_dataset(
        spec, 2025, base=tmp_path, runner=lambda a: calls.append(a), exists_check=lambda t, r: True
    )
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


def _standings(tmp_path):
    spec = REGISTRY["standings"]  # manifested: parquet + rds + csv + manifest
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    return spec, publish._dataset_files(spec, 2025, tmp_path)


def _publish_against(monkeypatch, tmp_path, spec, listing):
    monkeypatch.setattr(publish, "_release_digests", lambda tag, repo: listing)
    calls = []
    res = publish.publish_dataset(spec, 2025, base=tmp_path, runner=calls.append)
    return res, [Path(c[3]).name for c in calls if c[:2] == ["release", "upload"]], calls


def test_unchanged_parquet_skips_season_even_though_rds_and_manifest_restamped(
    tmp_path, monkeypatch
):
    spec, files = _standings(tmp_path)
    listing = {f.name: "sha256:yesterdays-bytes" for f in files}
    pq = next(f for f in files if f.suffix == ".parquet")
    listing[pq.name] = publish._sha256(pq)
    res, uploads, calls = _publish_against(monkeypatch, tmp_path, spec, listing)
    assert res["uploaded"] == 0 and calls == []  # no upload, no sidecar stamp


def test_changed_parquet_uploads_changed_files_parquet_last_then_stamps(tmp_path, monkeypatch):
    spec, files = _standings(tmp_path)
    listing = {f.name: publish._sha256(f) for f in files}
    pq = next(f for f in files if f.suffix == ".parquet")
    rds = next(f for f in files if f.suffix == ".rds")
    listing[pq.name] = listing[rds.name] = "sha256:stale"
    res, uploads, _calls = _publish_against(monkeypatch, tmp_path, spec, listing)
    assert uploads[:2] == [rds.name, pq.name]  # csv + manifest unchanged -> skipped
    assert len(uploads) == 2 + 4 and res["uploaded"] == 2


def test_missing_sibling_uploads_even_when_parquet_matches(tmp_path, monkeypatch):
    spec, files = _standings(tmp_path)
    pq = next(f for f in files if f.suffix == ".parquet")
    _res, uploads, _calls = _publish_against(
        monkeypatch, tmp_path, spec, {pq.name: publish._sha256(pq)}
    )
    assert uploads[: len(files)][-1] == pq.name and len(uploads) == len(files) + 4


def test_no_release_creates_it_then_uploads_everything(tmp_path, monkeypatch):
    spec, files = _standings(tmp_path)
    res, uploads, calls = _publish_against(monkeypatch, tmp_path, spec, None)
    assert calls[0][:3] == ["release", "create", spec.tag]
    assert res["uploaded"] == len(files) and uploads[len(files) - 1].endswith(".parquet")
