"""Republish the hoopR#23-deduped player_box seasons + post-publish verify (2026-08-26).

Uploads the already-rebuilt-and-GATED local files for each affected season via
``mbb_data_build.publish.publish_dataset`` (parquet + rds + on-the-fly csv +
manifest, tag ``espn_mens_college_basketball_player_boxscores``), then
downloads the freshly released parquet and re-runs the dual-team dupe
invariant plus a row-count match against the local file.

Rate-limit discipline: a failed ``gh`` call (403 / rate / transient network)
is retried with a linear backoff; it is NEVER treated as release-missing.

Run AFTER 20260826_hoopr23_player_box_dedupe_gate.py passes:

    uv run python ops/oneoff/20260826_hoopr23_republish_affected_seasons.py
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

import polars as pl  # noqa: E402
from mbb_data_build.config import REGISTRY  # noqa: E402
from mbb_data_build.publish import publish_dataset  # noqa: E402

SEASONS = [2014, 2015, 2017, 2018, 2020, 2021, 2022, 2025]
SPEC = REGISTRY["player_box"]
RELEASE_URL = (
    "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
    f"{SPEC.tag}/player_box_{{y}}.parquet"
)
MAX_TRIES = 6
BACKOFF_S = 120


def publish_with_retry(y: int) -> None:
    for attempt in range(1, MAX_TRIES + 1):
        try:
            publish_dataset(SPEC, y, base="mbb")
            return
        except subprocess.CalledProcessError as e:
            # gh exit != 0: rate limit / 403 / transient — wait and retry,
            # never conclude the release is missing.
            if attempt == MAX_TRIES:
                raise
            wait = BACKOFF_S * attempt
            print(f"{y}: publish attempt {attempt} failed ({e}); retrying in {wait}s")
            time.sleep(wait)


def verify_released(y: int) -> list[str]:
    fails: list[str] = []
    url = RELEASE_URL.format(y=y)
    tmp = Path(tempfile.mkdtemp(prefix="hoopr23_verify_")) / f"player_box_{y}.parquet"
    for attempt in range(1, MAX_TRIES + 1):
        rc = subprocess.run(
            ["curl", "-sSfL", "--retry", "3", "-o", str(tmp), url], timeout=600
        ).returncode
        if rc == 0:
            break
        if attempt == MAX_TRIES:
            return [f"{y}: could not download released parquet after {MAX_TRIES} tries"]
        time.sleep(BACKOFF_S)
    rel = pl.read_parquet(tmp)
    local = pl.read_parquet(f"mbb/player_box/parquet/player_box_{y}.parquet")
    pairs = (
        rel.filter(pl.col("athlete_id").is_not_null())
        .group_by(["game_id", "athlete_id"])
        .agg(pl.col("team_id").n_unique().alias("nt"))
        .filter(pl.col("nt") > 1)
        .height
    )
    print(f"{y}: released rows={rel.height} local rows={local.height} dual-team pairs={pairs}")
    if pairs != 0:
        fails.append(f"{y}: released asset still has {pairs} dual-team pairs")
    if rel.height != local.height:
        fails.append(f"{y}: released rows {rel.height} != local rows {local.height}")
    return fails


def main() -> int:
    fails: list[str] = []
    for y in SEASONS:
        print(f"=== publishing player_box {y} ===")
        publish_with_retry(y)
    print("=== post-publish verification ===")
    for y in SEASONS:
        fails += verify_released(y)
    if fails:
        print("\nPOST-PUBLISH VERIFY FAILED:")
        for f in fails:
            print(" -", f)
        return 1
    print("\nPOST-PUBLISH VERIFY PASSED for", SEASONS)
    return 0


if __name__ == "__main__":
    sys.exit(main())
