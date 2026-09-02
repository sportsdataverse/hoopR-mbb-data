"""Win-probability enrichment of the season's pbp -- the ONLY publisher of the pbp asset.

The pbp release asset has exactly one writer: this step. The build stage
(``mbb_data_build --dataset pbp``) writes the plain season parquet into the
tree and does NOT publish it; this step reads that parquet back, appends the
two WP columns, rewrites parquet/csv/rds together and uploads. ``publish.py``
refuses a pbp parquet without the WP columns, so no code path can ship an
un-enriched asset.

Why the step is separate from the pbp build: the pregame prior needs the
season's **schedule** and **team_box**, which the daily driver builds AFTER pbp
(the build order is load-bearing: pbp -> shots; player_box ->
player_season_stats; schedules stamps flags from the built pbp/team_box). So
the enrichment runs once every input is in the tree -- and reads all three
inputs from the tree, never from the release (the release pbp is the PREVIOUS
run's asset by construction now).

History: between 2026-07-12 (columns verified present) and 2026-08-02 (absent
from every season) the nightly published the plain pbp and re-applied WP
afterwards, so every publish briefly stripped the columns and a failed
re-application left them stripped -- which broke the platform's
win-probability page. The 2026-08-26 whole-history republish then stripped
every season but the current one. Single writer + publish guard closes both
windows.

Publishing goes through the normal ``io.write_dataset`` + ``publish.publish_dataset``
path so **parquet, csv and rds are all regenerated together**. Writing only the
parquet is how the formats drift: ``hoopR::load_mbb_*`` reads ``.rds`` exclusively,
so a parquet-only republish leaves every R user on un-enriched data from a
release that looks fresh.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import polars as pl

from mbb_data_build import io, publish
from mbb_data_build._logging import get_logger
from mbb_data_build.config import REGISTRY

log = get_logger()

WP_COLS = publish.WP_COLS

# The identity of a play in the published pbp. A compile that drops one event and
# duplicates another keeps the row count, the schema and the dtypes -- every check
# below the row count would pass it -- so the identities are compared as a multiset.
PLAY_KEY = ("game_id", "game_play_number")


def _tree_parquet(dataset: str, season: int, base: Path) -> Path:
    spec = REGISTRY[dataset]
    return io.dataset_dir(spec, base) / "parquet" / f"{spec.stem}_{season}.parquet"


def _default_compile(
    league: str,
) -> Callable[[pl.DataFrame, pl.DataFrame, pl.DataFrame], pl.DataFrame]:
    # Frame-level entry of the sdv-py season WP builder (``build_mbb_season_wp``
    # is this exact call over the RELEASE loaders). Private names, deliberately:
    # the release-based public entry would enrich the previous run's pbp here.
    from sportsdataverse.mbb.mbb_team_ratings import _normalize_schedule
    from sportsdataverse.mbb.mbb_win_prob import _compile_season_wp

    def compile_(pbp: pl.DataFrame, schedule: pl.DataFrame, team_box: pl.DataFrame) -> pl.DataFrame:
        return _compile_season_wp(pbp, _normalize_schedule(schedule), team_box, league=league)

    return compile_


def enrich_and_publish(
    season: int,
    *,
    league: str = "mens",
    base: str | Path = "mbb",
    dry_run: bool = False,
    compile: Callable[[pl.DataFrame, pl.DataFrame, pl.DataFrame], pl.DataFrame] | None = None,
) -> bool:
    """Append the WP columns to the season's tree pbp and publish all three formats.

    Reads ``pbp`` (required), ``schedules`` and ``team_box`` (optional -- the
    engine falls back to its HFA-only anchor without them) from the tree under
    ``base``. Returns True when the season was published (or would be, under
    ``dry_run``), and True with nothing to do when no pbp was built for the
    season (pre-season runs). Returns False -- never raises -- on any failure;
    the caller (``daily_mbb_data_processor.sh``) treats False as a failed
    season, because a pbp that is not enriched is a pbp that is not published.

    Args:
        season: Season (end-year convention).
        league: ``"mens"`` / ``"womens"`` (selects the sdv-py constants).
        base: Tree root holding ``pbp/``, ``schedules/``, ``team_box/``.
        dry_run: Plan the publish without ``gh`` calls (still writes nothing).
        compile: Injectable ``(pbp, schedule, team_box) -> enriched pbp`` for
            hermetic tests; defaults to the sdv-py season compile.
    """
    base = Path(base)
    spec = REGISTRY["pbp"]
    pq = _tree_parquet("pbp", season, base)
    if not pq.exists():
        log.info("wp %s %s: no pbp built under %s; nothing to enrich", league, season, base)
        return True
    try:
        pbp = pl.read_parquet(pq)
        aux = {}
        for ds in ("schedules", "team_box"):
            p = _tree_parquet(ds, season, base)
            if not p.exists():
                # Built minutes earlier in the same run: absence is an upstream
                # failure, and the engine's HFA-only fallback would publish a flat
                # pregame prior as if it were fresh.
                log.error(
                    "wp %s %s: %s parquet missing under %s; pbp NOT published this run",
                    league,
                    season,
                    ds,
                    base,
                )
                return False
            aux[ds] = pl.read_parquet(p)
        run = compile or _default_compile(league)
        frame = run(pbp, aux["schedules"], aux["team_box"])
    except Exception as exc:  # noqa: BLE001 - reported as a failed season by the caller
        log.error(
            "wp %s %s: enrichment failed (%s); pbp NOT published this run", league, season, exc
        )
        return False

    missing = [c for c in WP_COLS if c not in frame.columns]
    if missing:
        log.error(
            "wp %s %s: compile returned no %s; pbp NOT published this run", league, season, missing
        )
        return False
    if frame.height != pbp.height:
        log.error(
            "wp %s %s: compile changed the row count %d -> %d; pbp NOT published",
            league,
            season,
            pbp.height,
            frame.height,
        )
        return False
    key = [c for c in PLAY_KEY if c in pbp.columns and c in frame.columns]
    if key and not frame.select(key).sort(key).equals(pbp.select(key).sort(key)):
        log.error(
            "wp %s %s: compile changed the play identities on %s (a drop plus a duplicate "
            "keeps the row count); pbp NOT published",
            league,
            season,
            key,
        )
        return False
    lost = {c: dt for c, dt in pbp.schema.items() if frame.schema.get(c) != dt}
    if lost:
        log.error(
            "wp %s %s: compile dropped or retyped input columns %s; pbp NOT published",
            league,
            season,
            lost,
        )
        return False

    if dry_run:
        log.info(
            "wp %s %s: would rewrite + publish %s_%s parquet/csv/rds (%d rows)",
            league,
            season,
            spec.stem,
            season,
            frame.height,
        )
        return True

    io.write_dataset(frame, spec, season, base=base)
    try:
        # publish_dataset re-reads the parquet just written and refuses it unless
        # both WP columns are present and finite -- the assertion is on the file.
        publish.publish_dataset(spec, season, base=base, dry_run=False)
    except publish.UnenrichedPbpError as exc:
        log.error("wp %s %s: %s", league, season, exc)
        return False
    log.info(
        "wp %s %s: published parquet+csv+rds with %s (%d rows)",
        league,
        season,
        list(WP_COLS),
        frame.height,
    )
    return True


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(prog="mbb_data_build.wp_enrich")
    p.add_argument("-s", "--start", type=int, required=True)
    p.add_argument("-e", "--end", type=int, required=True)
    p.add_argument("--league", default="mens", choices=("mens", "womens"))
    p.add_argument("--base", default="mbb")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    ok = True
    for season in range(a.start, a.end + 1):
        ok = enrich_and_publish(season, league=a.league, base=a.base, dry_run=a.dry_run) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
