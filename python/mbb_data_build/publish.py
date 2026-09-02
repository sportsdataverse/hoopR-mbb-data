"""Release publishing -- per-file ``gh release upload --clobber`` (create-if-missing).

Port of the R ``sportsdataverse_save`` upload. Multi-asset globs silently drop
large files, so upload one file at a time. ``runner``/``exists_check`` are
injectable for hermetic tests.

MBB deltas vs the WNBA publisher: pbp/team_box/player_box never commit a
local csv (``spec.write_tree_csv=False`` -- R's ``fwrite`` for those is
commented out), so their release-asset csv is generated from the parquet into
a temp file at publish time instead of read off the tree; the per-dataset
``mbb_<ds>_in_data_repo.csv`` manifest is uploaded alongside for the
manifested datasets.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import Callable

import polars as pl

from mbb_data_build import io as build_io
from mbb_data_build._logging import get_logger, human_size
from mbb_data_build.config import DatasetSpec

_LEAGUE = "mbb"

DEFAULT_REPO = "sportsdataverse/sportsdataverse-data"

# Win-probability contract for the pbp release asset. ``wp_enrich`` appends the
# two WP columns in place; a pbp parquet WITHOUT them is the un-enriched
# intermediate, and uploading it is exactly the 2026-07/08 strip incident (every
# nightly overwrote the enriched asset with the plain build, and the platform's
# WP page broke). The guard reads the FILE that would be uploaded -- not the
# frame that produced it -- so any caller, any code path, is covered.
WP_COLS = ("pregame_home_prob", "home_win_prob")
# Observed 2026-09-01 on the release: 2026 -- the ONLY season still carrying the
# columns -- is 100% finite (0 nulls, 0 NaN over 2,915,731 plays, both columns);
# 2003/2006/2012/2016/2020/2024/2025 had lost the columns entirely (the incident
# this guard exists to catch). Floor set just below the observed 1.0 -- a real
# enrichment scores every play; a partial one is a bug, not a state.
WP_MIN_FINITE_RATE = 0.999

log = get_logger()


class UnenrichedPbpError(ValueError):
    """The pbp parquet about to be uploaded lacks (or barely carries) the WP columns."""


def assert_wp_enriched(
    parquet: Path,
    *,
    cols: tuple[str, ...] = WP_COLS,
    min_finite_rate: float = WP_MIN_FINITE_RATE,
) -> dict[str, float]:
    """Refuse a pbp parquet that is not WP-enriched; return the per-column finite rates.

    Checks the on-disk file (columns present, then the finite -- non-null AND
    non-NaN -- share of each WP column) so the assertion is on the OUTPUT that
    ships, never on which code path ran.

    Raises:
        UnenrichedPbpError: A WP column is missing, is not float-typed, or its
            finite rate is below ``min_finite_rate``.
    """
    lf = pl.scan_parquet(parquet)
    schema = lf.collect_schema()
    missing = [c for c in cols if c not in schema]
    if missing:
        raise UnenrichedPbpError(
            f"{parquet.name}: missing WP columns {missing} -- refusing to publish an "
            "un-enriched pbp asset (run wp_enrich first)"
        )
    # A numeric-looking STRING column ("0.62") casts cleanly to 0.62, so the
    # finite-rate check below would pass it and publish_dataset would upload a
    # string-typed WP column (the cast never touches the parquet). Reject the
    # native dtype first: a WP probability is a float column or it is wrong.
    mistyped = {c: str(schema[c]) for c in cols if not schema[c].is_float()}
    if mistyped:
        raise UnenrichedPbpError(
            f"{parquet.name}: WP columns are not float-typed: {mistyped} -- refusing to "
            "publish; a numeric string would satisfy the finite-rate floor while shipping "
            "a string column to consumers"
        )
    # is_finite: null -> null (dropped by sum), NaN and +/-inf -> False. strict=False
    # is belt-and-braces now that the dtype is proven numeric.
    counts = lf.select(
        pl.len().alias("_n"),
        *[pl.col(c).cast(pl.Float64, strict=False).is_finite().sum().alias(c) for c in cols],
    ).collect()
    n = int(counts["_n"][0])
    rates = {c: (int(counts[c][0]) / n if n else 0.0) for c in cols}
    low = {c: r for c, r in rates.items() if r < min_finite_rate}
    if low:
        raise UnenrichedPbpError(
            f"{parquet.name}: WP columns below the {min_finite_rate:.3f} finite-rate floor: "
            f"{ {c: round(r, 4) for c, r in low.items()} } over {n} plays -- refusing to publish"
        )
    return rates


def _gh(args: list[str]) -> None:
    # timeout so a hung gh (auth prompt, network stall, rate-limit backoff) can't
    # block an unattended pipeline step indefinitely. Args are internal literals /
    # controlled fields passed as a list (no shell=True) -- the SAST injection flag
    # is a false positive. 1800s, not 120: a full-coverage-era MBB season csv
    # (~500MB, e.g. play_by_play_2013.csv) legitimately takes 10+ minutes to
    # upload -- the 2026-07 name backfill died at 120s AND at 600s mid-season.
    subprocess.run(["gh", *args], check=True, timeout=1800)


def _gh_release_exists(tag: str, repo: str) -> bool:
    return (
        subprocess.run(
            ["gh", "release", "view", tag, "--repo", repo],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
        ).returncode
        == 0
    )


def _manifest_asset(spec: DatasetSpec, base: Path) -> Path | None:
    """Collapse the manifest append-log to one row per season for the release.

    R's manifest upload helper uploads
    ``distinct(season, .keep_all = TRUE) %>% arrange(season)`` -- and dplyr's
    distinct keeps the FIRST occurrence, so the published row_count freezes at
    a season's first-ever run. We keep the LATEST row per season instead, so
    the manifest describes the asset actually published alongside it. That is
    a deliberate divergence from R; completed seasons are unaffected (their
    first run == their last).
    """
    src = build_io.manifest_path(spec, base)
    if spec.manifest_endpoint is None or not src.exists():
        return None
    latest = (
        pl.read_csv(src).unique(subset=["season"], keep="last", maintain_order=True).sort("season")
    )
    tmp = Path(tempfile.mkdtemp(prefix="mbb_manifest_")) / src.name
    latest.write_csv(tmp)
    return tmp


def _dataset_files(spec: DatasetSpec, season: int, base: Path) -> list[Path]:
    # dataset_dir, not base/spec.dataset: the crosswalks live in the shared
    # mbb/crosswalk/ dir (spec.out_dir).
    root = build_io.dataset_dir(spec, base)
    pq = root / "parquet" / f"{spec.stem}_{season}.parquet"
    files = [pq] if pq.exists() else []
    # .rds is hoopR::load_mbb_*'s only read path -- publishing the
    # parquet without it silently freezes every downstream loader.
    rds = root / "rds" / f"{spec.stem}_{season}.rds"
    if rds.exists():
        files.append(rds)
    if spec.write_tree_csv:
        csv = root / "csv" / f"{spec.stem}_{season}.csv"
        if csv.exists():
            files.append(csv)
    elif pq.exists():
        # No committed tree csv (pbp/team_box/player_box) -- the release
        # asset contract still ships a plain .csv, generated on the fly.
        tmp = Path(tempfile.mkdtemp(prefix="mbb_publish_")) / f"{spec.stem}_{season}.csv"
        pl.read_parquet(pq).write_csv(tmp)
        files.append(tmp)
    # Manifest asset name == file name (R manifest_upload_helper contract).
    manifest = _manifest_asset(spec, base)
    if manifest is not None:
        files.append(manifest)
    return files


def publish_dataset(
    spec: DatasetSpec,
    season: int,
    *,
    base: str | Path = "mbb",
    repo: str = DEFAULT_REPO,
    dry_run: bool = False,
    runner: Callable[[list[str]], None] | None = None,
    exists_check: Callable[[str, str], bool] | None = None,
) -> dict:
    """Upload a dataset/season's parquet + csv to the release, creating it if missing.

    Args:
        spec: Dataset spec (``dataset``/``stem``/``tag``) from ``config.REGISTRY``.
        season: Season year; must match the files already written by ``io.write_dataset``.
        base: Root directory containing ``{dataset}/{parquet,csv}/...``.
        repo: ``owner/repo`` slug for the release target.
        dry_run: If True, skip all ``gh`` calls and print the would-be uploads.
        runner: Injectable ``gh`` arg-list executor; defaults to a real subprocess call.
        exists_check: Injectable ``(tag, repo) -> bool`` release-existence check.

    Returns:
        dict: ``{"tag": ..., "files": [...], "uploaded": <count>}``.

    Example:
        Quick start::

            from mbb_data_build.config import REGISTRY
            from mbb_data_build import publish
            publish.publish_dataset(REGISTRY["team_box"], 2025)
    """
    run = runner or _gh
    exists = exists_check or _gh_release_exists
    if spec.dataset == "pbp":
        # Before anything else (before the on-the-fly csv is even generated):
        # the pbp asset ships WP-enriched or not at all. Applies to dry runs
        # too -- a dry run that would be refused for real says so.
        pq = build_io.dataset_dir(spec, Path(base)) / "parquet" / f"{spec.stem}_{season}.parquet"
        if not pq.exists():
            raise UnenrichedPbpError(
                f"{pq.name}: no pbp parquet under {base}; refusing to publish a pbp release "
                "asset from leftover files"
            )
        rates = assert_wp_enriched(pq)
        log.info("%s %s: WP contract ok -- finite rates %s", spec.dataset, season, rates)
    files = _dataset_files(spec, season, Path(base))
    if not files:
        log.warning("%s %s: no files to publish under %s", spec.dataset, season, base)
    if not dry_run and not exists(spec.tag, repo):
        log.info("release %s missing on %s -- creating it", spec.tag, repo)
        run(
            [
                "release",
                "create",
                spec.tag,
                "--repo",
                repo,
                "--title",
                spec.tag,
                "--notes",
                f"{spec.tag} (MBB dataset, Python-built).",
            ]
        )
    count = 0
    for f in files:
        size = human_size(f.stat().st_size)
        if dry_run:
            log.info("[dry-run] upload %s (%s) -> %s:%s", f, size, repo, spec.tag)
            continue
        log.info("uploading %s (%s) -> %s:%s", f.name, size, repo, spec.tag)
        run(["release", "upload", spec.tag, str(f), "--repo", repo, "--clobber"])
        count += 1
        log.info("uploaded %s -> %s (asset %d/%d)", f.name, spec.tag, count, len(files))
    return {"tag": spec.tag, "files": [str(f) for f in files], "uploaded": count}
