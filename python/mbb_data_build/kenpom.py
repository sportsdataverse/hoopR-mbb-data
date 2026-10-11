"""KenPom game + player ids for the MBB schedule and player crosswalks.

KenPom's per-team pages are a paid feed, captured into the private
``hoopR-kp-data`` checkout (``data/team_schedules/csv``, ``data/players/csv``).
Only the ids and the join keys leave that repo; the KenPom stats never do.

* Games: one KenPom row per team-game, keyed by ``(season, GameId)``. Both teams
  map to ESPN ids through the season's team crosswalk (``kp_team`` ->
  ``espn_team_id``) and join on ``(game_date, unordered ESPN team pair)``.
* Players: ``PlayerId`` joins on the normalized name within ``(season, team)``;
  a name shared by two players on one team is broken by jersey.

A missing checkout (CI, a fresh machine) skips the leg with a warning: the
crosswalk still builds, with ``kp_game_id`` / ``kp_player_id`` null.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import polars as pl
from sportsdataverse._common_crosswalk_basketball import normalize_name

from mbb_data_build._logging import get_logger

log = get_logger()

RELEASE = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_crosswalk"


def kp_root() -> Path:
    """The hoopR-kp-data checkout: ``HOOPR_KP_DATA_ROOT`` or the sibling of this repo."""
    env = os.environ.get("HOOPR_KP_DATA_ROOT")
    return Path(env) if env else Path(__file__).resolve().parents[3] / "hoopR-kp-data"


def _kp_csv(kind: str, season: int) -> pl.DataFrame | None:
    path = kp_root() / "data" / kind / "csv" / f"{kind}_{season}.csv"
    if not path.exists():
        log.warning("KenPom %s %s not found at %s; kp ids left null", kind, season, path)
        return None
    return pl.read_csv(path, infer_schema_length=0)  # all Utf8: ids stay exact


def _team_map(season: int, base: Path) -> pl.DataFrame:
    """``kp_team`` -> ``espn_team_id`` (Int32) from the season's team crosswalk."""
    local = Path(base) / "crosswalk" / "parquet" / f"mbb_team_crosswalk_{season}.parquet"
    if local.exists():
        teams = pl.read_parquet(local)
    else:
        import requests

        resp = requests.get(f"{RELEASE}/mbb_team_crosswalk_{season}.parquet", timeout=60)
        resp.raise_for_status()
        teams = pl.read_parquet(io.BytesIO(resp.content))
    return (
        teams.select("kp_team", pl.col("espn_team_id").cast(pl.Int32))
        .drop_nulls()
        .unique(subset="kp_team", keep="none")
    )


def _kp_team_games(season: int, base: Path) -> pl.DataFrame | None:
    """KenPom team-game rows: ``game_date``, ``a`` (team), ``b`` (opponent, null when non-D-I), ``kp_game_id``."""
    kp = _kp_csv("team_schedules", season)
    if kp is None:
        return None
    teams = _team_map(season, base)
    return (
        kp.filter(pl.col("GameId").is_not_null() & (pl.col("GameId") != "NA"))
        .join(teams.rename({"kp_team": "Team", "espn_team_id": "a"}), on="Team", how="inner")
        .join(teams.rename({"kp_team": "Opponent", "espn_team_id": "b"}), on="Opponent", how="left")
        .select(
            pl.col("GameDate").str.strptime(pl.Date, "%Y%m%d").alias("game_date"),
            "a",
            "b",
            pl.col("GameId").cast(pl.Int64).cast(pl.Utf8).alias("kp_game_id"),
        )
        .unique()
    )


def add_kp_game_id(df: pl.DataFrame, season: int, base: Path) -> pl.DataFrame:
    """Fill ``kp_game_id`` on a schedule crosswalk frame; columns and dtypes unchanged.

    Pass 1 joins on (date, unordered ESPN team pair). Pass 2 covers games whose
    opponent KenPom can't map to ESPN (non-D-I): (date, the one mapped team),
    used only when that team has exactly one game that day on both sides.
    """
    kp = _kp_team_games(season, base)
    if kp is None or df.height == 0:
        return df
    pairs = (
        kp.drop_nulls("b")
        .select(
            "game_date",
            pl.min_horizontal("a", "b").alias("lo"),
            pl.max_horizontal("a", "b").alias("hi"),
            "kp_game_id",
        )
        .unique()
        .filter(pl.len().over("game_date", "lo", "hi") == 1)
    )
    one_a_day = (
        kp.select("game_date", "a", "kp_game_id")
        .unique()
        .filter(pl.len().over("game_date", "a") == 1)
    )
    left = (
        df.drop("kp_game_id")
        .with_row_index("_row")
        .with_columns(
            pl.min_horizontal("home_espn_team_id", "away_espn_team_id").alias("lo"),
            pl.max_horizontal("home_espn_team_id", "away_espn_team_id").alias("hi"),
        )
    )
    p1 = left.join(pairs, on=["game_date", "lo", "hi"], how="inner").select("_row", "kp_game_id")
    rest = left.join(p1, on="_row", how="anti")
    team_games = pl.concat(
        [
            left.select("_row", "game_date", pl.col(c).alias("a"))
            for c in ("home_espn_team_id", "away_espn_team_id")
        ]
    )
    single = team_games.filter(pl.len().over("game_date", "a") == 1)
    p2 = (
        rest.select("_row")
        .join(single, on="_row")
        .join(one_a_day, on=["game_date", "a"])
        .select("_row", "kp_game_id")
        .unique()
        .filter(pl.len().over("_row") == 1)
        .join(p1, on="kp_game_id", how="anti")
    )
    p2 = p2.filter(pl.len().over("kp_game_id") == 1)
    out = left.join(pl.concat([p1, p2]), on="_row", how="left").sort("_row").select(df.columns)
    assert out.height == df.height, "kp_game_id join changed the row count"
    log.info(
        "schedule_crosswalk %s: kp_game_id %d/%d (pair %d, single-team %d)",
        season,
        out["kp_game_id"].is_not_null().sum(),
        out.height,
        p1.height,
        p2.height,
    )
    return out


def _name_key(col: str) -> pl.Expr:
    # normalize_name, then drop spaces so "A.J. Patterson" (a j patterson) meets "AJ Patterson".
    return pl.col(col).map_elements(normalize_name, return_dtype=pl.Utf8).str.replace_all(" ", "")


def add_kp_player_id(df: pl.DataFrame, season: int, base: Path) -> pl.DataFrame:
    """Add ``kp_player_id`` (Utf8) after ``yahoo_player_name`` on a player crosswalk frame.

    Pass 1: exact normalized name within the team, jersey breaking a repeated
    name. Pass 2, for what's left on both sides: last name + jersey, unique
    within the team (nicknames: "Maurice"/"Moe"). KenPom lists only players who
    logged minutes, so ESPN roster rows without minutes stay null.
    """
    cols = list(df.columns)
    cols.insert(cols.index("yahoo_player_name") + 1, "kp_player_id")
    kp = _kp_csv("players", season)
    if kp is None or df.height == 0:
        return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("kp_player_id")).select(cols)
    teams = _team_map(season, base)
    players = (
        kp.join(teams.rename({"kp_team": "Team"}), on="Team", how="inner")
        .select(
            "espn_team_id",
            _name_key("Player").alias("key"),
            pl.col("Player")
            .map_elements(normalize_name, return_dtype=pl.Utf8)
            .str.split(" ")
            .list.last()
            .alias("last"),
            pl.col("Number").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("jersey"),
            pl.col("PlayerId").alias("kp_player_id"),
        )
        .filter(pl.col("key") != "")
        .unique(subset=["espn_team_id", "kp_player_id"])
    )
    left = df.with_row_index("_row").select(
        "_row",
        "espn_team_id",
        pl.col("player_name").str.replace_all(" ", "").alias("key"),
        pl.col("player_name").str.split(" ").list.last().alias("last"),
        pl.col("espn_jersey").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("jersey"),
    )

    def one_to_one(cand: pl.DataFrame) -> pl.DataFrame:
        cand = cand.filter(pl.len().over("_row") == 1)
        return cand.filter(pl.len().over("espn_team_id", "kp_player_id") == 1).select(
            "_row", "kp_player_id"
        )

    c1 = left.join(players.drop("last"), on=["espn_team_id", "key"], suffix="_kp")
    c1 = c1.filter((pl.len().over("_row") == 1) | (pl.col("jersey") == pl.col("jersey_kp")))
    m1 = one_to_one(c1)
    rest_l = left.join(m1, on="_row", how="anti").drop_nulls("jersey")
    rest_r = players.join(m1, on="kp_player_id", how="anti").drop_nulls("jersey")
    m2 = one_to_one(
        rest_l.join(
            rest_r.select("espn_team_id", "last", "jersey", "kp_player_id"),
            on=["espn_team_id", "last", "jersey"],
        )
    )
    out = (
        df.with_row_index("_row")
        .join(pl.concat([m1, m2]), on="_row", how="left")
        .sort("_row")
        .select(cols)
    )
    assert out.height == df.height, "kp_player_id join changed the row count"
    log.info(
        "player_crosswalk %s: kp_player_id %d/%d (name %d, last+jersey %d; KenPom players %d)",
        season,
        out["kp_player_id"].is_not_null().sum(),
        out.height,
        m1.height,
        m2.height,
        players.height,
    )
    return out
