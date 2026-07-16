"""Per-dataset reshapers -- each takes one game's final.json + returns a frame.

Every reshaper delegates the actual reshape to a ``sportsdataverse.mbb``
producer (thin league shims over the shared WBB/NBA basketball
implementations); this module is just the registry + per-game glue.
Signature contract: ``(final, *, season, game_id) -> pl.DataFrame``.

Deltas vs the hoopR-nba-data/nba_data_build template (one step removed from
the wehoop-wnba-data original):

* **No draft.** MBB (college) has no draft dataset -- there is no
  ``draft_builder`` / ``draft`` entry in ``RESHAPERS``/``SEASON_BUILDERS``.
* ``officials_builder`` reads the ``game_rosters/json`` sidecar (NOT its own
  ``officials/json`` raw dir -- MBB doesn't have one) via
  ``helper_mbb_officials`` (re-exported from ``sportsdataverse.nba``, same
  as NBA -- officials is a league-neutral dataset).
* ``player_season_stats_builder`` builds its identity lookup from the
  season's already-compiled ``player_box`` parquet
  (``build_mbb_player_identity_lookup``, re-exported from
  ``sportsdataverse.nba.build_nba_player_identity_lookup``), not from team
  rosters -- ESPN's player_season_stats payload is flat/full-career and
  carries no reliable season-scoped identity of its own. This means
  pbp -> team_box -> player_box -> player_season_stats is a real build-order
  dependency for MBB (same NBA delta; WBB/WNBA have no such ordering
  constraint).
* **No season-level postprocess needed.** Unlike NBA (whose pbp/team_box
  season unions can lack ``type_abbreviation``/``largest_lead`` when no game
  in the union happens to carry them), the MBB producer's per-game
  ``helper_mbb_play_by_play``/``helper_mbb_team_box`` always emit their full
  fixed column set (``largest_lead`` is part of the WBB-shared final select,
  not a conditional insert; the MBB pbp oracle carries no
  ``type_abbreviation`` column at all) -- ``SEASON_POSTPROCESS`` is empty.
  ``media_id`` is likewise NOT a postprocess concern: it is a real (mostly
  null) per-play field the raw payload carries (``plays[].mediaId``), and
  the payload-first-seen column union in ``helper_mbb_play_by_play``
  surfaces it automatically -- no backfill required.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from sportsdataverse.mbb import (
    helper_mbb_play_by_play,
    helper_mbb_player_box,
    helper_mbb_schedule,
    helper_mbb_team_box,
)

from mbb_data_build._logging import get_logger

log = get_logger()


def team_box_reshaper(final: dict, *, season: int, game_id: int) -> pl.DataFrame:
    return helper_mbb_team_box(final)


def pbp_reshaper(final: dict, *, season: int, game_id: int) -> pl.DataFrame:
    return helper_mbb_play_by_play(final)


def player_box_reshaper(final: dict, *, season: int, game_id: int) -> pl.DataFrame:
    """helper_mbb_player_box() emits the MBB release column order natively.

    The released MBB player_box orders ``active`` LAST, which sdv-py now handles
    in ``mbb_player_box._MBB_FINAL_ORDER`` (on the pinned main), so no local
    reordering is needed here.
    """
    return helper_mbb_player_box(final)


RESHAPERS: dict = {
    "team_box": team_box_reshaper,
    "pbp": pbp_reshaper,
    "player_box": player_box_reshaper,
}

# --- season-level builders (no per-game loop) --------------------------------
# Signature contract: (season, *, raw_root, base) -> pl.DataFrame. Each reads
# the raw season tree and/or the already-built parquets under ``base``.

_SHOTS_COLS = (
    "game_id",
    "season",
    "period_number",
    "clock_display_value",
    "team_id",
    "athlete_id_1",
    "athlete_id_2",
    "type_id",
    "type_text",
    "scoring_play",
    "score_value",
    "coordinate_x",
    "coordinate_y",
    "coordinate_x_raw",
    "coordinate_y_raw",
)


def shots_from_pbp(pbp: pl.DataFrame) -> pl.DataFrame:
    """R espn_mbb_01 shots block: filter shooting plays, project the shot cols."""
    if pbp.is_empty():
        return pl.DataFrame()
    out = pbp.filter(pl.col("shooting_play") == True)  # noqa: E712
    return out.select([c for c in _SHOTS_COLS if c in out.columns])


def _built_game_ids(base: Path, dataset: str, stem: str, season: int) -> list[int]:
    p = base / dataset / "parquet" / f"{stem}_{season}.parquet"
    if not p.exists():
        # Fails open (every flag -> False), like R's empty-espn_df branch. Say so
        # loudly: if this is a pipeline-order violation (or a failed upstream
        # build) rather than a genuinely unbuilt season, the schedule would ship
        # PBP=FALSE for every game.
        log.warning(
            "%s %s: no built parquet at %s -- schedule flags for it will all be False",
            dataset,
            season,
            p,
        )
        return []
    return (
        pl.read_parquet(p, columns=["game_id"])
        .get_column("game_id")
        .cast(pl.Int64)
        .unique()
        .to_list()
    )


def schedules_builder(season: int, *, raw_root: Path | str, base: Path) -> pl.DataFrame:
    """Released schedule = raw schedule + casts/dates + PBP/team_box/player_box flags."""
    from mbb_data_build import ingest

    # raw_root may be a local Path or an HTTP base URL (ingest.raw_root -> Path | str);
    # use the dual-mode reader instead of ``raw_root / ...`` which TypeErrors on a str.
    raw = ingest._read_season_schedule(season, raw_root)
    if raw is None:
        return pl.DataFrame()
    return helper_mbb_schedule(
        raw,
        pbp_game_ids=_built_game_ids(base, "pbp", "play_by_play", season),
        team_box_game_ids=_built_game_ids(base, "team_box", "team_box", season),
        player_box_game_ids=_built_game_ids(base, "player_box", "player_box", season),
    )


def shots_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    """Shots derive from the already-built play_by_play parquet (no extra I/O in R)."""
    p = base / "pbp" / "parquet" / f"play_by_play_{season}.parquet"
    if not p.exists():
        return pl.DataFrame()
    return shots_from_pbp(pl.read_parquet(p))


def _sidecar_builder(subdir: str, helper, *, fallback_subdir: str | None = None) -> object:
    """Per-game sidecar loop (R scripts 09/10): completed games, tryCatch skips.

    ``fallback_subdir`` recovers a game from a second raw location when the
    primary sidecar is absent. Used by officials: its ``gameInfo.officials``
    block is byte-identical in the processed ``json/final`` summary (present for
    ~10x more historical games than the ``game_rosters/json`` scrape: 137k vs
    13k), so pre-scrape seasons compile officials with zero HTTP. Purely additive
    -- for a game whose primary sidecar exists the fallback never fires, so
    current-season output is unchanged.
    """

    def _build(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
        from mbb_data_build import ingest

        def _one(gid: int) -> pl.DataFrame | None:
            payload = ingest.read_final(gid, raw_root=raw_root, subdir=subdir)
            if payload is None and fallback_subdir is not None:
                payload = ingest.read_final(gid, raw_root=raw_root, subdir=fallback_subdir)
            if payload is None:
                return None
            try:
                frame = helper(payload, season=season, game_id=gid)
            except Exception as e:  # R tryCatch(...) -> NULL parity
                log.warning("%s: parse failed for game %s: %s", subdir, gid, e)
                return None
            return frame if frame.height else None

        # Thread-pooled reads (I/O-bound HTTP in CI); input-order results keep
        # the concat byte-identical to the serial build.
        gids = ingest.season_completed_game_ids(season, raw_root=raw_root)
        frames = [f for f in ingest.parallel_map(_one, gids) if f is not None]
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal_relaxed")

    return _build


def _game_rosters_builder() -> object:
    from sportsdataverse.mbb import helper_mbb_game_rosters

    # Same zero-HTTP backfill as officials: fall back to json/final when the
    # recent-only game_rosters/json scrape is absent. The boxscore roster is
    # structurally identical there; the only observed divergence is occasional
    # display-name formatting ("L.J. Cryer" vs "LJ Cryer", 1/31 on the checked
    # game), and historical seasons have no game_rosters/json oracle, so
    # json/final is the authoritative source for them.
    return _sidecar_builder(
        "game_rosters/json", helper_mbb_game_rosters, fallback_subdir="json/final"
    )


def _officials_builder() -> object:
    # MBB has no mbb/officials/ raw dir -- officials are projected from the
    # SAME game_rosters sidecar that backs helper_mbb_game_rosters.
    from sportsdataverse.mbb import helper_mbb_officials

    # Officials' gameInfo.officials block is identical in json/final (verified),
    # so fall back there to recover pre-scrape seasons (game_rosters/json is
    # recent-only) -- same json/final fallback as game_rosters above.
    return _sidecar_builder("game_rosters/json", helper_mbb_officials, fallback_subdir="json/final")


def _per_entity_frames(
    subdir: str, season: int, raw_root: Path, helper, id_kw: str
) -> list[pl.DataFrame]:
    """R scripts 04/06/07: loop the season's per-entity JSONs, tryCatch skips."""
    from mbb_data_build import ingest

    def _one(eid: int) -> pl.DataFrame | None:
        payload = ingest.read_final(eid, raw_root=raw_root, subdir=f"{subdir}/json/{season}")
        if payload is None:
            return None
        try:
            frame = helper(payload, **{"season": season, id_kw: eid})
        except Exception as e:  # R tryCatch(...) -> NULL parity
            log.warning("%s: parse failed for entity %s: %s", subdir, eid, e)
            return None
        return frame if frame.height else None

    # Thread-pooled reads; input-order results keep _season_concat deterministic.
    eids = ingest.season_dir_ids(subdir, season, raw_root=raw_root)
    return [f for f in ingest.parallel_map(_one, eids) if f is not None]


def _season_concat(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame()
    # R: season-level distinct().
    return pl.concat(frames, how="diagonal_relaxed").unique(maintain_order=True, keep="first")


def rosters_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    from sportsdataverse.mbb import helper_mbb_rosters

    return _season_concat(
        _per_entity_frames("team_rosters", season, raw_root, helper_mbb_rosters, "team_id")
    )


def team_season_stats_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    from sportsdataverse.mbb import helper_mbb_team_season_stats

    return _season_concat(
        _per_entity_frames("team_stats", season, raw_root, helper_mbb_team_season_stats, "team_id")
    )


def player_season_stats_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    """MBB delta (same as NBA): identity comes from the season's already-built
    player_box parquet (``build_mbb_player_identity_lookup``), not team
    rosters -- ESPN's player_season_stats payload is flat/full-career and
    cannot answer "who played in season Y" on its own. Requires player_box to
    have been built first under ``base``.
    """
    from sportsdataverse.mbb import (
        build_mbb_player_identity_lookup,
        helper_mbb_player_season_stats,
    )

    from mbb_data_build import ingest

    pb_path = base / "player_box" / "parquet" / f"player_box_{season}.parquet"
    if pb_path.exists():
        lookup = build_mbb_player_identity_lookup(pl.read_parquet(pb_path))
    else:
        log.warning(
            "player_season_stats %s: no built player_box parquet at %s -- "
            "identity columns will be blank (build player_box first)",
            season,
            pb_path,
        )
        lookup = {}

    def _helper(payload: dict, *, season: int, athlete_id: int) -> pl.DataFrame:
        return helper_mbb_player_season_stats(
            payload, season=season, athlete_id=athlete_id, identity_lookup=lookup
        )

    # R's build_season_player_stats() iterates ONLY the identity lookup's
    # athlete ids (i.e. athletes who actually appear in the season's built
    # player_box) -- NOT every athlete json ever scraped into the flat
    # player_season_stats/ raw tree. Iterating the whole flat directory
    # instead would emit extra rows (with blank identity) for athletes whose
    # career-stats file happens to carry a season==Y statistics entry despite
    # never appearing in that season's player_box (confirmed against the
    # 2025 oracle: 2 such athletes, 77 extra rows).
    athlete_ids = sorted({int(k) for k in lookup})

    def _one(aid: int) -> pl.DataFrame | None:
        payload = ingest.read_final(aid, raw_root=raw_root, subdir="player_season_stats/json")
        if payload is None:
            return None
        try:
            frame = _helper(payload, season=season, athlete_id=aid)
        except Exception as e:  # R tryCatch(...) -> NULL parity
            log.warning("player_season_stats: parse failed for athlete %s: %s", aid, e)
            return None
        return frame if frame.height else None

    # Thread-pooled athlete reads (thousands per season over HTTP in CI);
    # input-order results keep _season_concat deterministic.
    frames = [f for f in ingest.parallel_map(_one, athlete_ids) if f is not None]
    return _season_concat(frames)


def standings_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    from sportsdataverse.mbb import helper_mbb_standings

    from mbb_data_build import ingest

    payload = ingest.read_final(season, raw_root=raw_root, subdir="standings/json")
    if payload is None:
        return pl.DataFrame()
    out = helper_mbb_standings(payload, season=season)
    # espn_mbb_07:190-ish ends in dplyr::distinct(). A team nested under both a
    # conference and a league group would double up without this.
    return out.unique(maintain_order=True)


SEASON_BUILDERS: dict = {
    "schedules": schedules_builder,
    "shots": shots_builder,
    "game_rosters": _game_rosters_builder(),
    "officials": _officials_builder(),
    "rosters": rosters_builder,
    "team_season_stats": team_season_stats_builder,
    "player_season_stats": player_season_stats_builder,
    "standings": standings_builder,
}


# --- season-level post-processing (after the per-game concat) -----------------

# MBB needs no season-level column backfills (see module docstring): the
# per-game reshapers always emit their full fixed column set, so a season
# union never comes up short a column the way NBA's pbp/team_box can.
SEASON_POSTPROCESS: dict = {}
