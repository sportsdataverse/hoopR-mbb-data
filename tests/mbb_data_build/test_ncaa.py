"""stats.ncaa.org id legs of the schedule + player crosswalks (``mbb_data_build.ncaa``).

Fixture rows are copied from the published 2024 / 2025 files (``ncaa_mbb_schedule_2024``,
``mbb_crosswalk/ncaa_espn_team_crosswalk``, ``mbb_schedule_crosswalk_2024``,
``mbb_player_crosswalk_2025``, ``mbb_groups/mbb_player_aliases``); the one synthetic row is
marked where it is made.
"""

from __future__ import annotations

from datetime import date

import polars as pl
from mbb_data_build import ncaa

TEAMS = pl.DataFrame(
    {
        "season": ["2023-24"] * 7 + ["2024-25"],
        "ncaa_team": [
            "Indiana", "FGCU", "SIUE", "Nicholls", "Murray St.", "Minnesota", "IUPUI", "IU Indy",
        ],
        "espn_team_id": ["84", "526", "2565", "2447", "93", "135", "85", "85"],
    }
)  # fmt: skip

SCHEDULE = pl.DataFrame(
    {
        "contest_id": ["3727922", "3956555", "3954939", "3954498"],
        "game_date": ["11/07/2023", "11/17/2023", "11/06/2023", "12/12/2023"],
        "home": ["Indiana", "SIUE", "Murray St.", "Minnesota"],
        "away": ["FGCU", "Nicholls", "Midway", "IU Indy"],
        "home_score": [69, 51, 91, 101],
        "away_score": [63, 60, 58, 65],
        "season": [2024] * 4,
    }
)


def _xwalk(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "season": pl.Int32,
            "game_date": pl.Date,
            "home_espn_team_id": pl.Int32,
            "away_espn_team_id": pl.Int32,
            "espn_game_id": pl.Utf8,
            "kp_game_id": pl.Utf8,
            "fox_game_id": pl.Utf8,
            "yahoo_game_id": pl.Utf8,
            "match_method": pl.Utf8,
        },
        orient="row",
    )


def _contest_ids(df: pl.DataFrame, **kw) -> list:
    kw.setdefault("schedule", SCHEDULE)
    out = ncaa.add_ncaa_contest_id(df, 2024, teams=TEAMS, **kw)
    assert out.columns.index("ncaa_contest_id") == out.columns.index("yahoo_game_id") + 1
    assert out.drop("ncaa_contest_id").equals(df)
    assert out.schema["ncaa_contest_id"] == pl.Utf8
    return out["ncaa_contest_id"].to_list()


def test_pass1_pair_and_neutral_site_swap():
    df = _xwalk(
        [
            (2024, date(2023, 11, 7), 84, 526, "401577612", None, None, None, "espn+bart"),
            # ESPN lists Nicholls home; stats.ncaa.org lists SIUE home.
            (2024, date(2023, 11, 17), 2447, 2565, "401604346", None, None, None, "espn+bart"),
            # Same pair, a day off: no match.
            (2024, date(2023, 11, 8), 84, 526, "1", None, None, None, "espn"),
        ]
    )
    assert _contest_ids(df) == ["3727922", "3956555", None]


def test_pass2_single_mapped_team_against_non_d1():
    # Midway is not in the NCAA->ESPN team crosswalk; ESPN has its own id for it.
    df = _xwalk([(2024, date(2023, 11, 6), 93, 111166, "401594428", None, None, None, "espn")])
    assert _contest_ids(df) == ["3954939"]


def test_pass2_rejects_two_different_d1_opponents():
    # ESPN says Indiana played Nicholls that day; NCAA says FGCU. Both mapped: a conflict.
    df = _xwalk([(2024, date(2023, 11, 7), 84, 2447, "x", None, None, None, "espn")])
    assert _contest_ids(df) == [None]


def test_renamed_team_falls_back_to_another_seasons_name():
    # The 2024 schedule says "IU Indy"; the 2023-24 crosswalk row says "IUPUI".
    df = _xwalk([(2024, date(2023, 12, 12), 135, 85, "401594216", None, None, None, "espn")])
    assert _contest_ids(df) == ["3954498"]


def test_no_schedule_keeps_null_column_and_rerun_is_stable():
    df = _xwalk([(2009, date(2008, 11, 14), 84, 526, "1", None, None, None, "espn")])
    out = ncaa.add_ncaa_contest_id(df, 2009, schedule=None, teams=TEAMS)  # pre-2010: no source
    assert out["ncaa_contest_id"].to_list() == [None]
    assert out.columns.index("ncaa_contest_id") == out.columns.index("yahoo_game_id") + 1
    again = ncaa.add_ncaa_contest_id(out, 2009, schedule=None, teams=TEAMS)
    assert again.columns == out.columns


def test_join_key_dtype_mismatch_fails_loudly():
    df = _xwalk([(2024, date(2023, 11, 7), 84, 526, "1", None, None, None, "espn")])
    bad = df.with_columns(pl.col("home_espn_team_id").cast(pl.Int64))
    try:
        ncaa.add_ncaa_contest_id(bad, 2024, schedule=SCHEDULE, teams=TEAMS)
    except AssertionError as e:
        assert "home_espn_team_id" in str(e)
    else:
        raise AssertionError("an Int64 team id must not join an Int32 one silently")


PLAYERS = pl.DataFrame(
    {
        "season": pl.Series([2025, 2025, 2025], dtype=pl.Int32),
        "espn_team_id": pl.Series([150, 2377, 2492], dtype=pl.Int32),
        "player_name": ["cooper flagg", "mike saunders", "mike saunders"],
        "espn_athlete_id": ["5041939", "4432403", "4432403"],
        "yahoo_player_name": [None, None, None],
        "kp_player_id": [None, None, None],
        "match_method": ["espn_only"] * 3,
    }
)

ALIASES = pl.DataFrame(
    {
        "athlete_id": ["5041939", "4432403", "4432403", "4432403"],
        "source": ["ncaa", "ncaa", "ncaa", "kenpom"],
        "source_player_id": ["9035807", "8373880", "9090605", "70001"],
        "team_id": ["150", "2377", "2492", "2492"],
        "valid_from": pl.Series([2025, 2024, 2025, 2025], dtype=pl.Int32),
        "valid_to": pl.Series([2025, 2024, 2025, 2025], dtype=pl.Int32),
    }
)


def test_player_id_season_window_and_team_preference():
    out = ncaa.add_ncaa_player_id(PLAYERS, 2025, aliases=ALIASES)
    assert out.columns.index("ncaa_player_id") == out.columns.index("kp_player_id") + 1
    assert out.drop("ncaa_player_id").equals(PLAYERS)
    # Saunders is listed on both ESPN teams; his one 2025 NCAA id was made on 2492, so the
    # 2377 row stays null (the 2024 id is outside the window, the kenpom row another source).
    assert out["ncaa_player_id"].to_list() == ["9035807", None, "9090605"]


def test_player_multiple_ncaa_ids_prefer_the_rows_team():
    # Synthetic: a second 2025 NCAA id for Saunders, made on 2377 (a mid-season transfer).
    two = pl.concat(
        [
            ALIASES,
            ALIASES.slice(1, 1).with_columns(
                pl.lit("9999999").alias("source_player_id"), pl.lit(2025, pl.Int32).alias("valid_from"),
                pl.lit(2025, pl.Int32).alias("valid_to"),
            ),
        ]
    )  # fmt: skip
    out = ncaa.add_ncaa_player_id(PLAYERS, 2025, aliases=two)
    assert out["ncaa_player_id"].to_list() == ["9035807", "9999999", "9090605"]
    # Two ids, neither on the row's team: ambiguous, left null.
    other = PLAYERS.with_columns(pl.lit(99, pl.Int32).alias("espn_team_id")).slice(1, 1)
    assert ncaa.add_ncaa_player_id(other, 2025, aliases=two)["ncaa_player_id"].to_list() == [None]


def test_player_no_aliases_keeps_null_column():
    out = ncaa.add_ncaa_player_id(PLAYERS, 2009, aliases=ALIASES)
    assert out["ncaa_player_id"].null_count() == 3
    assert out.columns.index("ncaa_player_id") == out.columns.index("kp_player_id") + 1


def test_season_label():
    assert ncaa.season_label(2024) == "2023-24" and ncaa.season_label(2010) == "2009-10"
    assert ncaa.season_label(2000) == "1999-00"
