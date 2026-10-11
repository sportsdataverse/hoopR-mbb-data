"""KenPom-only refresh of the published current-season MBB schedule + player crosswalks.

The full build runs on GitHub-hosted runners, which have no hoopR-kp-data checkout (and
barttorvik.com 403s the droplet, so the full build cannot move here). This droplet step
reads the crosswalks the full build already published, recomputes ``kp_game_id`` /
``kp_player_id`` from hoopR-kp-data, keeps a previously published id where the new pass
finds none, and writes (+ publishes) only when an id changed.

    uv run python -m mbb_data_build.kp_fill -s 2027 --base ../mbb [--publish | --dry-run]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from mbb_data_build import io, kenpom, publish
from mbb_data_build._logging import get_logger
from mbb_data_build.config import REGISTRY

log = get_logger()

#: dataset -> (KenPom csv kind, id column)
LEGS = {
    "schedule_crosswalk": ("team_schedules", "kp_game_id"),
    "player_crosswalk": ("players", "kp_player_id"),
}


def fill(
    dataset: str,
    season: int,
    *,
    base: str | Path = "mbb",
    publish_release: bool = False,
    dry_run: bool = False,
) -> pl.DataFrame | None:
    """Re-join one published crosswalk's KenPom ids; None when there is nothing to fill.

    Returns:
        pl.DataFrame | None: The filled frame, or None when the season has no published
        crosswalk or no KenPom capture yet (preseason) -- both logged, neither an error.
    """
    base = Path(base)
    spec = REGISTRY[dataset]
    kind, col = LEGS[dataset]
    prev = kenpom.published(spec.stem, season, base)
    if prev is None:
        log.warning("%s %s: no published crosswalk yet; nothing to fill", dataset, season)
        return None
    if not (kenpom.kp_root() / "data" / kind / "csv" / f"{kind}_{season}.csv").exists():
        log.warning("%s %s: no KenPom %s capture yet; nothing to fill", dataset, season, kind)
        return None
    if dataset == "schedule_crosswalk":
        fresh = kenpom.add_kp_game_id(prev, season, base)
    else:
        fresh = kenpom.add_kp_player_id(prev.drop(col), season, base)
    out = kenpom.carry_forward(fresh, col, season, base, prev=prev)
    assert out.columns == prev.columns and out.height == prev.height
    log.info(
        "%s %s: %s %d/%d (published %d)",
        dataset,
        season,
        col,
        out[col].is_not_null().sum(),
        out.height,
        prev[col].is_not_null().sum(),
    )
    if out.equals(prev):
        log.info("%s %s: KenPom ids unchanged; nothing written", dataset, season)
        return out
    io.write_dataset(out, spec, season, base=base)
    if publish_release or dry_run:
        publish.publish_dataset(spec, season, base=base, dry_run=dry_run)
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="mbb_data_build.kp_fill")
    p.add_argument("-s", "--start", type=int, required=True)
    p.add_argument("-e", "--end", type=int, default=None)
    p.add_argument("--base", default="mbb")
    p.add_argument("--dataset", choices=sorted(LEGS), action="append")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--publish", action="store_true")
    g.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    for season in range(a.start, (a.end or a.start) + 1):
        for ds in a.dataset or sorted(LEGS, reverse=True):  # schedule, then player
            fill(ds, season, base=a.base, publish_release=a.publish, dry_run=a.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
