"""Build the MBB model-dataset parquet for the sportsdataverse-data release tags.

Thin orchestration over the ``sportsdataverse.mbb`` compute surface, mirroring
the cfb/pwhl `*_model_publish` builders:

* :func:`build_ratings` -> one ``mbb_ratings_{season}.parquet`` per season
  (``mbb_team_ratings``: AdjO/AdjD/AdjEM/AdjTempo per team-season).
* :func:`build_player_value` -> one ``mbb_player_value_{season}.parquet`` per
  season (``mbb_box_bpm``: per-player-season box Plus/Minus).

Both compute fns load the released ESPN inputs themselves (schedule + team
boxscores / player boxscores), so the builder needs no local data tree and is
GH-Actions friendly.
"""

from __future__ import annotations

import json
from pathlib import Path

# Probed against the released assets (2026-07-16): 2002 has no team_boxscore
# at all (the loader returns a column-less empty frame), and 2003-2005 are
# junk-sparse (2 / 23 / <100 teams). Real coverage starts at 2006 (526 teams,
# monotonically up to ~700 by 2025).
MIN_SEASON = 2006


def _build_seasonal(
    seasons: list[int],
    out_dir,
    *,
    stem: str,
    compute,
) -> list[dict]:
    """Shared season loop: compute -> refuse-empty -> write ``{stem}_{season}.parquet``."""
    too_old = [s for s in seasons if s < MIN_SEASON]
    if too_old:
        raise ValueError(f"{stem}: seasons {too_old} predate the {MIN_SEASON} ESPN boxscore floor")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []
    for season in seasons:
        df = compute(season)
        if df.height == 0:
            raise ValueError(
                f"{stem}: season {season} produced 0 rows -- refusing to publish an empty tag"
            )
        path = out_dir / f"{stem}_{season}.parquet"
        df.write_parquet(path)
        results.append({"season": season, "rows": df.height, "path": str(path)})
        print(f"{stem}: season={season} rows={df.height} -> {path}")
    return results


def build_ratings(seasons: list[int], out_dir, *, compute=None) -> list[dict]:
    """Build per-season team ratings and write ``mbb_ratings_{season}.parquet``.

    Args:
        seasons: Seasons to build (hoopR end-year convention; one parquet per
            season).
        out_dir: Output directory (created if absent).
        compute: Injectable ``mbb_team_ratings``-shaped callable, for hermetic
            tests. Defaults to ``sportsdataverse.mbb.mbb_team_ratings`` with
            ``league="mens"``.

    Returns:
        List of ``{"season": int, "rows": int, "path": str}`` dicts, in input
        order.

    Raises:
        ValueError: If a season is below :data:`MIN_SEASON` or yields zero rows.
    """
    if compute is None:
        from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings

        def compute(season):
            return mbb_team_ratings(season, league="mens")

    return _build_seasonal(seasons, out_dir, stem="mbb_ratings", compute=compute)


def build_player_value(seasons: list[int], out_dir, *, compute=None) -> list[dict]:
    """Build per-season box-BPM tables and write ``mbb_player_value_{season}.parquet``.

    Args:
        seasons: Seasons to build (hoopR end-year convention).
        out_dir: Output directory (created if absent).
        compute: Injectable ``mbb_box_bpm``-shaped callable, for hermetic
            tests. Defaults to ``sportsdataverse.mbb.mbb_box_bpm`` with
            ``league="mens"``.

    Returns:
        List of ``{"season": int, "rows": int, "path": str}`` dicts, in input
        order.

    Raises:
        ValueError: If a season is below :data:`MIN_SEASON` or yields zero rows.
    """
    if compute is None:
        from sportsdataverse.mbb import mbb_box_bpm

        def compute(season):
            return mbb_box_bpm(season, league="mens")

    return _build_seasonal(seasons, out_dir, stem="mbb_player_value", compute=compute)


def write_ratings_card(results: list[dict], out_dir) -> Path:
    """Write the ``mbb_ratings`` model card next to the season parquet."""
    return _write_card(
        results,
        out_dir,
        tag="mbb_ratings",
        grain="one row per team per season",
        source=(
            "sdv-py sportsdataverse.mbb.mbb_team_ratings(league='mens') over the "
            "released ESPN schedule + team boxscores"
        ),
        notes=[
            "AdjO/AdjD are opponent-adjusted points per 100 possessions;"
            " adj_em = adj_o - adj_d; rank is dense on adj_em descending.",
            "The adjustment fixed point and constants are gated in sdv-py's"
            " T1.1 oracle suite; this tag materializes that compute unchanged.",
        ],
    )


def write_player_value_card(results: list[dict], out_dir) -> Path:
    """Write the ``mbb_player_value`` model card next to the season parquet."""
    return _write_card(
        results,
        out_dir,
        tag="mbb_player_value",
        grain="one row per (player_id, season, team_id)",
        source=(
            "sdv-py sportsdataverse.mbb.mbb_box_bpm(league='mens') over the "
            "released ESPN player boxscores"
        ),
        notes=[
            "Box Plus/Minus with the team constraint: minutes-weighted player"
            " scores sum to the team's adjusted efficiency margin (points per"
            " 100 possessions above league average).",
            "Coefficients are the bundled team-constrained artifact gated in"
            " sdv-py's T1.2 oracle suite; this tag materializes that compute"
            " unchanged.",
        ],
    )


def _write_card(results, out_dir, *, tag, grain, source, notes) -> Path:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    card = {
        "tag": tag,
        "grain": grain,
        "source": source,
        "seasons": [r["season"] for r in results],
        "rows_by_season": {str(r["season"]): r["rows"] for r in results},
        "notes": notes,
    }
    path = out_dir / f"{tag}_card.json"
    path.write_text(json.dumps(card, indent=2) + "\n", encoding="utf-8")
    print(f"card: {path}")
    return path
