"""Committed Bart Torvik team lists for the crosswalk builds.

barttorvik.com's CloudFront answers 403 to every datacenter IP -- GitHub
runners and the droplet alike -- and 200 only to residential ones. The team
crosswalk needs one Torvik file per season, ``{season}_team_results.csv`` (its
``team`` / ``conf`` columns are the Torvik join), so that file is fetched from
a residential machine and committed under ``torvik/`` at the repo root, with
its URL, fetch time and sha256 in ``torvik/manifest.json``. The build reads
the committed file and never calls barttorvik.com.

Refresh (residential IP only; at least once per season, before it starts)::

    cd python && uv run python -m mbb_data_build.torvik_snapshot -s 2027 -e 2027
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from sportsdataverse._crosswalk_basketball_sources import TORVIK_FIRST_SEASON
from sportsdataverse.mbb.torvik_parsers import parse_torvik_csv

from mbb_data_build._logging import get_logger

log = get_logger()

ROOT = Path(__file__).resolve().parents[2] / "torvik"
URL = "https://barttorvik.com/{season}_team_results.csv"


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


def refresh(seasons: range) -> None:
    """Fetch each season's team_results.csv into ``torvik/`` and record its provenance."""
    from sportsdataverse.mbb.torvik import torvik_ratings

    manifest_path = ROOT / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    ROOT.mkdir(exist_ok=True)
    for season in seasons:
        # Torvik serves CRLF; git stores LF, so hash and write what is committed.
        text = torvik_ratings(year=season, return_parsed=False).replace("\r\n", "\n")
        n = _teams(text, URL.format(season=season)).height  # never commit a blocked/empty body
        name = f"{season}_team_results.csv"
        (ROOT / name).write_text(text, encoding="utf-8", newline="")
        manifest[name] = {
            "url": URL.format(season=season),
            "fetched_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "teams": n,
        }
        log.info("torvik %s: %d teams -> %s", season, n, name)
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
