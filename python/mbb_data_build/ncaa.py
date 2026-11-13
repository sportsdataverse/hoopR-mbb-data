"""stats.ncaa.org ids for the MBB schedule and player crosswalks.

Both sources are public sportsdataverse-data releases, read through sdv-py:

* Games: ``ncaa_mbb_schedule_{season}`` lists one row per contest with NCAA team
  NAMES. Names map to ESPN team ids through ``ncaa_espn_team_crosswalk`` for the
  season label (2024 -> ``"2023-24"``), then join the crosswalk on
  ``(game_date, unordered ESPN team pair)`` -- home/away is not trusted, a
  neutral-site game can list either side first. Pass 2, as in ``kenpom``:
  ``(date, the one mapped team)`` when that team plays once that day on both sides.
* Players: ``mbb_player_aliases`` (``source == "ncaa"``) joins on the ESPN athlete
  id with ``season`` in ``[valid_from, valid_to]``. NCAA roster ids are new every
  season; when an athlete has more than one, the one made on the row's ESPN team wins.

A season with no source (before 2010, or an unpublished asset) keeps the column,
all null, and logs; the schema never depends on the source being there.
"""

from __future__ import annotations

import polars as pl

from mbb_data_build._logging import get_logger

log = get_logger()

LEAGUE = "mbb"
#: First season stats.ncaa.org schedules are published for.
FIRST_SEASON = 2010
#: New columns go right after these (the last provider id of each crosswalk).
GAME_ANCHOR = "yahoo_game_id"
PLAYER_ANCHOR = "kp_player_id"


def season_label(season: int) -> str:
    """The team crosswalk's season label: 2024 -> ``"2023-24"``."""
    return f"{season - 1}-{season % 100:02d}"


def _placed(df: pl.DataFrame, col: str, anchor: str) -> list[str]:
    """``df``'s columns with ``col`` after ``anchor`` (kept where it already is)."""
    if col in df.columns:
        return list(df.columns)
    cols = list(df.columns)
    cols.insert(cols.index(anchor) + 1 if anchor in cols else len(cols), col)
    return cols


def _null(df: pl.DataFrame, col: str, anchor: str) -> pl.DataFrame:
    return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col)).select(_placed(df, col, anchor))


def load_schedule(season: int) -> pl.DataFrame | None:
    """The season's published stats.ncaa.org schedule, or None (before 2010 / unpublished)."""
    if season < FIRST_SEASON:
        return None
    from sportsdataverse.mbb import load_ncaa_mbb_schedule

    df = load_ncaa_mbb_schedule(seasons=season)
    return df if df.height else None


def load_teams() -> pl.DataFrame:
    from sportsdataverse.mbb import ncaa_espn_team_crosswalk

    return ncaa_espn_team_crosswalk(league=LEAGUE)


def load_aliases() -> pl.DataFrame:
    from sportsdataverse.mbb import load_mbb_player_aliases

    return load_mbb_player_aliases()


def team_map(teams: pl.DataFrame, season: int) -> pl.DataFrame:
    """``ncaa_team`` -> ``espn_team_id`` (Int32).

    The season's own rows first; a name missing there falls back to any season
    where it names exactly one ESPN team. The published schedules carry today's
    names ("FDU", "IU Indy") on older seasons, which the season's rows spell
    "Fairleigh Dickinson" / "IUPUI".
    """
    named = teams.select(
        "season", "ncaa_team", pl.col("espn_team_id").cast(pl.Int32).alias("espn_team_id")
    ).drop_nulls()
    own = (
        named.filter(pl.col("season") == season_label(season))
        .select("ncaa_team", "espn_team_id")
        .unique(subset="ncaa_team", keep="none")
    )
    anywhere = (
        named.select("ncaa_team", "espn_team_id")
        .unique()
        .filter(pl.len().over("ncaa_team") == 1)
        .join(own, on="ncaa_team", how="anti")
    )
    return pl.concat([own, anywhere])


def ncaa_team_games(schedule: pl.DataFrame, teams: pl.DataFrame, season: int) -> pl.DataFrame:
    """One row per (contest, side): ``game_date``, ``a`` (team), ``b`` (opponent, null when unmapped), ``ncaa_contest_id``."""
    tm = team_map(teams, season)
    games = (
        schedule.select(
            pl.col("game_date").str.strptime(pl.Date, "%m/%d/%Y").alias("game_date"),
            "home",
            "away",
            # Int64 first: a non-numeric id fails here, and "0123" can't survive as a second key.
            pl.col("contest_id").cast(pl.Int64).cast(pl.Utf8).alias("ncaa_contest_id"),
        )
        .join(tm.rename({"ncaa_team": "home", "espn_team_id": "h"}), on="home", how="left")
        .join(tm.rename({"ncaa_team": "away", "espn_team_id": "v"}), on="away", how="left")
    )
    return (
        pl.concat(
            [
                games.select(
                    "game_date", pl.col("h").alias("a"), pl.col("v").alias("b"), "ncaa_contest_id"
                ),
                games.select(
                    "game_date", pl.col("v").alias("a"), pl.col("h").alias("b"), "ncaa_contest_id"
                ),
            ]
        )
        .drop_nulls("a")
        .unique()
    )


def add_ncaa_contest_id(
    df: pl.DataFrame,
    season: int,
    *,
    schedule: pl.DataFrame | None = None,
    teams: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Add ``ncaa_contest_id`` (Utf8) after ``yahoo_game_id`` on a schedule crosswalk frame.

    ``schedule`` / ``teams`` default to the published releases (tests and the
    dry-run pass them in). Re-running on a frame that already has the column
    recomputes it in place.
    """
    col = "ncaa_contest_id"
    cols = _placed(df, col, GAME_ANCHOR)
    schedule = load_schedule(season) if schedule is None else schedule
    if schedule is None or schedule.height == 0:
        log.warning("schedule_crosswalk %s: no stats.ncaa.org schedule; %s left null", season, col)
        return _null(df.drop(col, strict=False), col, GAME_ANCHOR).select(cols)
    if df.height == 0:
        return _null(df.drop(col, strict=False), col, GAME_ANCHOR).select(cols)
    games = ncaa_team_games(schedule, load_teams() if teams is None else teams, season)
    for k in ("home_espn_team_id", "away_espn_team_id"):
        assert df.schema[k] == games.schema["a"], f"{k}: {df.schema[k]} vs NCAA {games.schema['a']}"
    assert df.schema["game_date"] == games.schema["game_date"], "game_date dtype mismatch"
    pairs = (
        games.drop_nulls("b")
        .select(
            "game_date",
            pl.min_horizontal("a", "b").alias("lo"),
            pl.max_horizontal("a", "b").alias("hi"),
            col,
        )
        .unique()
        .filter(pl.len().over("game_date", "lo", "hi") == 1)
    )
    one_a_day = (
        games.select("game_date", "a", "b", col)
        .unique()
        .filter(pl.len().over("game_date", "a") == 1)
    )
    mapped = games["a"].unique().implode()
    left = (
        df.drop(col, strict=False)
        .with_row_index("_row")
        .with_columns(
            pl.min_horizontal("home_espn_team_id", "away_espn_team_id").alias("lo"),
            pl.max_horizontal("home_espn_team_id", "away_espn_team_id").alias("hi"),
        )
    )
    p1 = left.join(pairs, on=["game_date", "lo", "hi"], how="inner").select("_row", col)
    rest = left.join(p1, on="_row", how="anti")
    sides = (("home_espn_team_id", "away_espn_team_id"), ("away_espn_team_id", "home_espn_team_id"))
    team_games = pl.concat(
        [
            left.select("_row", "game_date", pl.col(c).alias("a"), pl.col(o).alias("o"))
            for c, o in sides
        ]
    )
    single = team_games.filter(pl.len().over("game_date", "a") == 1)
    p2 = (
        rest.select("_row")
        .join(single, on="_row")
        .join(one_a_day, on=["game_date", "a"])
        # Both sides name a different mapped D-I opponent: a conflict, not a non-D-I gap.
        # (One side unmapped is the gap: a non-D-I team, or an ESPN id the map spells
        # differently -- 2018 has ESPN 6446 for the crosswalk's 112358.)
        .filter(pl.col("b").is_null() | ~pl.col("o").is_in(mapped))
        .select("_row", col)
        .unique()
        .filter(pl.len().over("_row") == 1)
        .join(p1, on=col, how="anti")
    )
    p2 = p2.filter(pl.len().over(col) == 1)
    out = left.join(pl.concat([p1, p2]), on="_row", how="left").sort("_row").select(cols)
    assert out.height == df.height, f"{col} join changed the row count"
    log.info(
        "schedule_crosswalk %s: %s %d/%d (pair %d, single-team %d; NCAA contests %d)",
        season,
        col,
        out[col].is_not_null().sum(),
        out.height,
        p1.height,
        p2.height,
        schedule.height,
    )
    return out


def add_ncaa_player_id(
    df: pl.DataFrame, season: int, *, aliases: pl.DataFrame | None = None
) -> pl.DataFrame:
    """Add ``ncaa_player_id`` (Utf8) after ``kp_player_id`` on a player crosswalk frame.

    Joins the ESPN athlete id for ``season`` in ``[valid_from, valid_to]``. Among
    several NCAA ids the ones made on the row's ESPN team win; a row still left
    with two, or an NCAA id still claimed by two rows, stays null.
    """
    col = "ncaa_player_id"
    cols = _placed(df, col, PLAYER_ANCHOR)
    base = df.drop(col, strict=False)
    aliases = load_aliases() if aliases is None else aliases
    ncaa = aliases.filter(
        (pl.col("source") == "ncaa")
        & (pl.col("valid_from") <= season)
        & (pl.col("valid_to") >= season)
    ).select(
        pl.col("athlete_id").cast(pl.Int64).alias("_athlete"),
        pl.col("team_id").cast(pl.Int32).alias("_team"),
        pl.col("source_player_id").cast(pl.Int64).cast(pl.Utf8).alias(col),
    )
    if ncaa.height == 0 or df.height == 0:
        log.warning(
            "player_crosswalk %s: no stats.ncaa.org player aliases; %s left null", season, col
        )
        return _null(base, col, PLAYER_ANCHOR).select(cols)
    left = base.with_row_index("_row").select(
        "_row",
        pl.col("espn_athlete_id").cast(pl.Int64).alias("_athlete"),
        pl.col("espn_team_id").alias("_team_row"),
    )
    assert left.schema["_athlete"] == ncaa.schema["_athlete"]
    assert left.schema["_team_row"] == ncaa.schema["_team"], (
        f"espn_team_id: {left.schema['_team_row']} vs NCAA {ncaa.schema['_team']}"
    )
    cand = (
        left.join(ncaa.unique(), on="_athlete")
        .with_columns((pl.col("_team") == pl.col("_team_row")).alias("_same_team"))
        # per row: the same-team ids if any, then exactly one; per NCAA id the same, other way.
        .filter(pl.col("_same_team") == pl.col("_same_team").max().over("_row"))
        .filter(pl.len().over("_row") == 1)
        .filter(pl.col("_same_team") == pl.col("_same_team").max().over(col))
        .filter(pl.len().over(col) == 1)
        .select("_row", col)
    )
    out = base.with_row_index("_row").join(cand, on="_row", how="left").sort("_row").select(cols)
    assert out.height == df.height, f"{col} join changed the row count"
    log.info(
        "player_crosswalk %s: %s %d/%d (NCAA aliases for the season %d)",
        season,
        col,
        out[col].is_not_null().sum(),
        out.height,
        ncaa.height,
    )
    return out
