"""Committed Bart Torvik team lists and schedules for the crosswalk builds.

barttorvik.com's CloudFront answers 403 to every datacenter IP -- GitHub
runners and the droplet alike -- and 200 only to residential ones. The team
crosswalk needs one Torvik file per season, ``{season}_team_results.csv`` (its
``team`` / ``conf`` columns are the Torvik join). The schedule crosswalk also
needs ``{season}_super_sked.json``, Torvik's game list, which is the spine it
joins ESPN games onto. Both are fetched from a residential machine and committed
under ``torvik/`` at the repo root, with URL, fetch time and sha256 in
``torvik/manifest.json``. The builds read the committed files and never call
barttorvik.com.

Refresh (residential IP only; before the season starts, and again once the
conference and NCAA tournament games are scheduled, since the schedule snapshot
holds only the games Torvik listed when it was fetched)::

    cd python && uv run python -m mbb_data_build.torvik_snapshot -s 2027 -e 2027
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from sportsdataverse._crosswalk_basketball_sources import TORVIK_FIRST_SEASON, parse_super_sked
from sportsdataverse.mbb.torvik_parsers import parse_torvik_csv

from mbb_data_build._logging import get_logger

log = get_logger()

ROOT = Path(__file__).resolve().parents[2] / "torvik"
URL = "https://barttorvik.com/{season}_team_results.csv"
SKED_URL = "https://barttorvik.com/{season}_super_sked.json"


def _teams(text: str, what: str) -> pl.DataFrame:
    """``team`` / ``conf`` from a team_results CSV, refusing one with no teams."""
    raw = parse_torvik_csv(text)
    if not {"team", "conf"} <= set(raw.columns) or raw["team"].drop_nulls().len() == 0:
        raise ValueError(f"{what}: no Torvik team rows ({raw.height} rows)")
    return raw.select(pl.col("team").cast(pl.Utf8), pl.col("conf").cast(pl.Utf8))


def bart_teams(season: int) -> pl.DataFrame:
    """The season's Torvik ``team`` / ``conf`` from the committed snapshot.

    The same frame sdv-py's ``torvik_teams("mbb", season)`` fetches live, for
    ``mbb_team_crosswalk(bart=...)``. Zero rows before Torvik's first season,
    as sdv-py returns.

    Raises:
        FileNotFoundError: No snapshot for a season Torvik covers.
    """
    if season < TORVIK_FIRST_SEASON["mbb"]:
        return pl.DataFrame(schema={"team": pl.Utf8, "conf": pl.Utf8})
    path = ROOT / f"{season}_team_results.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} missing; from a residential IP run "
            f"`cd python && uv run python -m mbb_data_build.torvik_snapshot -s {season} -e {season}` and commit it"
        )
    return _teams(path.read_text(encoding="utf-8"), str(path))


def _games(text: str, what: str, season: int) -> pl.DataFrame:
    """Dated games from a super_sked JSON, refusing one with none."""
    games = parse_super_sked(text, season).filter(pl.col("game_date").is_not_null())
    if games.height == 0:
        raise ValueError(f"{what}: no dated Torvik games")
    return games


def bart_games(season: int) -> pl.DataFrame | None:
    """The season's Torvik super schedule from the committed snapshot, or ``None`` when none is committed.

    The same frame sdv-py's ``bart_super_sked("mbb", season)`` fetches live, for
    ``mbb_schedule_crosswalk(bart_games=...)``.
    """
    path = ROOT / f"{season}_super_sked.json"
    return _games(path.read_text(encoding="utf-8"), str(path), season) if path.exists() else None


def schedule_inputs(season: int) -> dict[str, pl.DataFrame]:
    """``bart`` / ``bart_games`` for ``mbb_schedule_crosswalk`` from the committed snapshot.

    Empty when the season has no committed schedule, so sdv-py fetches both live
    as it always did -- which works from a residential IP only. A committed
    schedule without its team list raises (see :func:`bart_teams`).
    """
    games = bart_games(season)
    return {} if games is None else {"bart": bart_teams(season), "bart_games": games}


def refresh(seasons: range) -> None:
    """Fetch each season's team_results.csv and super_sked.json into ``torvik/`` and record their provenance."""
    from sportsdataverse.mbb.torvik import torvik_ratings
    from sportsdataverse.mbb.torvik_runtime import _get

    manifest_path = ROOT / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    ROOT.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for season in seasons:
        # Torvik serves CRLF; git stores LF, so hash and write what is committed.
        text = torvik_ratings(year=season, return_parsed=False).replace("\r\n", "\n")
        n = _teams(text, URL.format(season=season)).height  # never commit a blocked/empty body
        name = f"{season}_team_results.csv"
        (ROOT / name).write_text(text, encoding="utf-8", newline="")
        manifest[name] = {
            "url": URL.format(season=season),
            "fetched_at_utc": stamp,
            "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "teams": n,
        }
        log.info("torvik %s: %d teams -> %s", season, n, name)
        # The runtime hands back parsed JSON; commit it compact, one line, as hashed.
        sked = json.dumps(_get(SKED_URL.format(season=season)), separators=(",", ":"))
        g = _games(sked, SKED_URL.format(season=season), season).height
        name = f"{season}_super_sked.json"
        (ROOT / name).write_text(sked, encoding="utf-8", newline="")
        manifest[name] = {
            "url": SKED_URL.format(season=season),
            "fetched_at_utc": stamp,
            "sha256": hashlib.sha256(sked.encode("utf-8")).hexdigest(),
            "games": g,
        }
        log.info("torvik %s: %d games -> %s", season, g, name)
    manifest_path.write_text(json.dumps(dict(sorted(manifest.items())), indent=2) + "\n")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("-s", "--start", type=int, required=True)
    p.add_argument("-e", "--end", type=int, required=True)
    a = p.parse_args(argv)
    refresh(range(max(a.start, TORVIK_FIRST_SEASON["mbb"]), a.end + 1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
