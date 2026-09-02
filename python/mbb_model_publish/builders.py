"""Build the MBB model-dataset parquet for the sportsdataverse-data release tags.

Thin orchestration over the ``sportsdataverse.mbb`` compute surface, mirroring
the cfb/pwhl `*_model_publish` builders:

* :func:`build_ratings` -> one ``mbb_ratings_{season}.parquet`` per season
  (``mbb_team_ratings``: AdjO/AdjD/AdjEM/AdjTempo per team-season), level-gated
  by :func:`assert_ratings_level`.
* :func:`build_player_value` -> one ``mbb_player_value_{season}.parquet`` per
  season (``mbb_box_bpm``: per-player-season box Plus/Minus) plus the ADDITIVE
  ``qualified`` flag; :func:`write_player_value_coefficients` ships the fitted
  coefficient vector beside the card.

Both compute fns load the released ESPN inputs themselves (schedule + team
boxscores / player boxscores), so the builder needs no local data tree and is
GH-Actions friendly.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Callable

import polars as pl

# Probed against the released assets (2026-07-16): 2002 has no team_boxscore
# at all (the loader returns a column-less empty frame), and 2003-2005 are
# junk-sparse (2 / 23 / <100 teams). Real coverage starts at 2006 (526 teams,
# monotonically up to ~700 by 2025).
MIN_SEASON = 2006

# ---------------------------------------------------------------------------
# player_value: the ADDITIVE ``qualified`` flag (never a filter). Derived
# 2026-09-01 from the published mbb_player_value 2014-2026 assets (115,271 rows):
# sd(box_bpm) by minutes bin falls 7.17 (0-25 min) -> 4.78 (100-150) -> 4.23
# (250-300) -> 4.08 (300-350) -> 3.93 (350-400) -> 3.73 (600-800) -> 3.43
# (1000-1400); 300 is the first bin within 10% of the 600-800 plateau. Same-
# player year-over-year r: 0.42 (all) / 0.72 (>=100) / 0.767 (>=300) / 0.785
# (>=500) -- diminishing past 300. >=300 keeps 88.2% of 2026 minutes (31.2% of
# rows). The engine's own fit floor (artifact ``min_minutes`` = 150) governs the
# team-sum weights only, not consumer noise.
QUALIFIED_MIN_MINUTES = 300.0

# ---------------------------------------------------------------------------
# ratings: absolute level bands -- the scale check a rank gate cannot do
# (Spearman is invariant to any monotone rescale; the sdv-py oracle gates are
# rank gates). Derived 2026-09-01 from the published mbb_ratings 2006-2026 plus
# in-season engine snapshots (2024/2025/2026 at Dec 10 -> May 1) over the
# QUALIFIED subset -- teams with >= 10 games, i.e. the D1 core; the full frame
# carries every opponent ever seen, whose few-game ratings run to -138. Observed:
#   qualified teams   334-366 end-of-season; 153+ from ~Dec 10
#   mean adj_o        101.9-111.1        mean adj_d      100.7-108.5
#   mean adj_em       0.09-6.0 (6.0 = Dec 20 2025-26; <= 1.9 end-of-season)
#   sd adj_em         13.2-16.7          mean adj_tempo  65.1-70.0
# Bands = observed range padded so a real season never trips them while a unit
# or scale bug does (per-game instead of per-100, a sign flip, an un-centred
# margin, or an all-NaN fixed point -- the published 2011 asset is 604/604 NaN).
RATINGS_LEVEL_BANDS: dict[str, tuple[float, float]] = {
    "adj_o": (95.0, 118.0),
    "adj_d": (95.0, 118.0),
    "adj_em": (-8.0, 8.0),
    "adj_tempo": (60.0, 76.0),
}
ADJ_EM_SD_BAND = (10.0, 22.0)
MIN_GAMES_GATED = 10
# Below this many qualified teams (early November) the season has no level yet;
# the gate logs that it did not apply instead of pretending to.
MIN_GATED_TEAMS = 150


def add_qualified(df: pl.DataFrame) -> pl.DataFrame:
    """Append ``qualified = min >= QUALIFIED_MIN_MINUTES`` (additive; no row is dropped)."""
    if "min" not in df.columns:
        raise ValueError(
            "player_value: frame has no 'min' column; cannot derive the qualified flag"
        )
    return df.with_columns((pl.col("min") >= QUALIFIED_MIN_MINUTES).alias("qualified"))


def assert_ratings_level(df: pl.DataFrame, season: int) -> dict:
    """Refuse a ratings frame whose qualified-subset levels sit outside the observed bands.

    Returns the per-season gate record (``applied``, the team count and, when
    applied, the measured means/sd) for the card sidecar.

    Raises:
        ValueError: A qualified team carries a non-finite rating, or a level or
            the adj_em spread is outside its band.
    """
    needed = ["games", *RATINGS_LEVEL_BANDS]
    if any(c not in df.columns for c in needed):
        raise ValueError(f"mbb_ratings: season {season}: frame lacks {needed}; cannot level-gate")
    q = df.filter(pl.col("games") >= MIN_GAMES_GATED)
    # Finiteness is checked on ANY qualified team, before the applicability floor:
    # an all-NaN fixed point (two such seasons are published today) must not ship
    # in November either. NaN is not null in polars, and is_nan() does NOT match
    # +/-inf, so the predicate is is_null() | ~is_finite(): on [1.0, nan, inf, -inf,
    # null] the old is_null()|is_nan() caught 2 of 4 bad values (polars 1.42.1).
    for c in RATINGS_LEVEL_BANDS:
        bad = q.filter(pl.col(c).is_null() | ~pl.col(c).is_finite()).height
        if bad:
            raise ValueError(
                f"mbb_ratings: season {season}: {bad}/{q.height} qualified teams have a "
                f"non-finite {c} -- refusing to publish"
            )
    if q.height < MIN_GATED_TEAMS:
        print(
            f"mbb_ratings: season {season}: level gate NOT applied -- {q.height} teams with "
            f">= {MIN_GAMES_GATED} games (< {MIN_GATED_TEAMS}); the season has no level yet"
        )
        return {"applied": False, "teams": q.height}
    stats = {f"mean_{c}": float(q[c].mean()) for c in RATINGS_LEVEL_BANDS}
    stats["sd_adj_em"] = float(q["adj_em"].std())
    checks = {f"mean_{c}": band for c, band in RATINGS_LEVEL_BANDS.items()}
    checks["sd_adj_em"] = ADJ_EM_SD_BAND
    out_of_band = {
        k: (round(stats[k], 3), band)
        for k, band in checks.items()
        if not (band[0] <= stats[k] <= band[1])
    }
    if out_of_band:
        raise ValueError(
            f"mbb_ratings: season {season}: level band violated over {q.height} qualified teams: "
            f"{out_of_band} (value, (lo, hi)) -- refusing to publish"
        )
    print(
        f"mbb_ratings: season {season}: level gate ok over {q.height} qualified teams: "
        + json.dumps({k: round(v, 3) for k, v in stats.items()})
    )
    return {"applied": True, "teams": q.height, **stats}


def _build_seasonal(
    seasons: list[int],
    out_dir,
    *,
    stem: str,
    compute,
    transform: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
    gate: Callable[[pl.DataFrame, int], dict] | None = None,
) -> list[dict]:
    """Shared season loop: compute -> refuse-empty -> transform -> gate -> write ``{stem}_{season}.parquet``."""
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
        if transform is not None:
            df = transform(df)
        meta = gate(df, season) if gate is not None else {}
        path = out_dir / f"{stem}_{season}.parquet"
        df.write_parquet(path)
        row = {"season": season, "rows": df.height, "path": str(path)}
        if meta:
            row["gate"] = meta
        results.append(row)
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
        List of ``{"season": int, "rows": int, "path": str, "gate": {...}}``
        dicts, in input order.

    Raises:
        ValueError: If a season is below :data:`MIN_SEASON`, yields zero rows,
            or fails :func:`assert_ratings_level`.
    """
    if compute is None:
        from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings

        def compute(season):
            return mbb_team_ratings(season, league="mens")

    return _build_seasonal(
        seasons, out_dir, stem="mbb_ratings", compute=compute, gate=assert_ratings_level
    )


def build_player_value(seasons: list[int], out_dir, *, compute=None) -> list[dict]:
    """Build per-season box-BPM tables and write ``mbb_player_value_{season}.parquet``.

    Every published column is preserved; the additive ``qualified`` flag
    (``min >= QUALIFIED_MIN_MINUTES``) is appended.

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

    return _build_seasonal(
        seasons, out_dir, stem="mbb_player_value", compute=compute, transform=add_qualified
    )


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
            " T1.1 oracle suite (rank gates); this tag materializes that compute unchanged.",
            "Publish level gate (scale check beside the rank gates): over teams with"
            f" >= {MIN_GAMES_GATED} games, mean adj_o/adj_d/adj_em/adj_tempo and sd adj_em"
            f" must sit inside {RATINGS_LEVEL_BANDS} / {ADJ_EM_SD_BAND}; applied once"
            f" >= {MIN_GATED_TEAMS} teams qualify (per-season record under gates_by_season).",
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
            " unchanged and ships the vector as mbb_player_value_coefficients.json.",
            f"`qualified` = min >= {QUALIFIED_MIN_MINUTES:g} (additive flag, no row"
            " dropped): the floor where sd(box_bpm) first sits within 10% of its"
            " high-minute plateau on the published 2014-2026 assets.",
        ],
    )


def write_player_value_coefficients(out_dir, *, league: str = "mens", load=None) -> Path:
    """Ship the fitted box-BPM coefficient vector as ``mbb_player_value_coefficients.json``.

    A copy of sdv-py's bundled ``{prefix}_box_bpm`` artifact (feature list,
    intercept + slopes for O and D on standardized z-clipped features, the
    standardization moments, lambdas, fit floor, train seasons, provenance) plus
    the sportsdataverse version and artifact sha256 it came from. Uploaded by the
    ``mbb_player_value_*.*`` pattern beside the season parquet + card.
    """
    if load is None:
        from sportsdataverse.mbb.mbb_player_value_constants import (
            get_player_value_constants,
            load_artifact,
        )

        def load():
            return load_artifact(f"{get_player_value_constants(league).bundle_prefix}_box_bpm")

    art = load()
    payload = json.dumps(art, sort_keys=True).encode("utf-8")
    try:
        import importlib.metadata as md

        version = md.version("sportsdataverse")
    except Exception:  # noqa: BLE001 - version is provenance, never a failure
        version = None
    out = {
        "tag": "mbb_player_value",
        "artifact": f"sportsdataverse/mbb/models/{art.get('league', league)}_box_bpm.json"
        if art.get("league") in (None, league)
        else "sportsdataverse bundled box_bpm artifact",
        "sportsdataverse_version": version,
        "artifact_sha256": hashlib.sha256(payload).hexdigest(),
        "written": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "note": (
            "obpm_coef / dbpm_coef are [intercept, *slopes] over feature_cols after "
            "standardizing with feature_mean / feature_sd and clipping at +/- z_clip; "
            "a slope is therefore the BPM change per one SD of the feature, so |slope| "
            "is coefficient importance."
        ),
        **art,
    }
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "mbb_player_value_coefficients.json"
    path.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print(f"coefficients: {path}")
    return path


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
    gates = {str(r["season"]): r["gate"] for r in results if r.get("gate")}
    if gates:
        card["gates_by_season"] = gates
    path = out_dir / f"{tag}_card.json"
    path.write_text(json.dumps(card, indent=2) + "\n", encoding="utf-8")
    print(f"card: {path}")
    return path
