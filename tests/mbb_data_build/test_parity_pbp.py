"""Parity: Python play_by_play vs a FROZEN, R-built oracle (four real 2025 games).

Port provenance: ``hoopR:::helper_espn_mbb_pbp``
(``hoopR-mbb-data/R/espn_mbb_01_pbp_creation.R``).

Oracle: ``tests/fixtures/pbp_r_oracle/play_by_play_r_oracle.parquet``, built by
``build_r_oracle.R`` (hoopR + R pinned in that folder's README) from the raw
ESPN payloads committed beside it. It used to be the tree asset
``mbb/pbp/parquet/play_by_play_2025.parquet``, but the daily processor has
written that file from Python since 2026-10-18 (3a0fb5a2), so the test was
comparing Python to Python. The fixture is never written by the producer.

R's play ``id`` is jsonlite's Float64 (18-digit ESPN ids overflow the double
mantissa); the producer writes exact Int64. That one upgrade is pinned and the
values are compared through the oracle's lossy Float64 view.
"""

import gzip
import json
from pathlib import Path

import polars as pl
from mbb_data_build.reshapers import pbp_reshaper

from tests.mbb_data_build._parity_helpers import assert_parquet_parity

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "pbp_r_oracle"
ORACLE = FIXTURE / "play_by_play_r_oracle.parquet"
GAMES = (401721337, 401721734, 401724552, 401746065)

KEYS = ["game_id", "game_play_number"]
# Joined per game from boxscore.players (2026-07); hoopR has no counterpart.
NAME_COLS = ("athlete_name_1", "athlete_name_2", "athlete_name_3")


def _build_py() -> pl.DataFrame:
    # Same reshape + diagonal_relaxed union as build.build_season's per-game loop.
    frames = []
    for gid in GAMES:
        with gzip.open(FIXTURE / "raw" / f"{gid}.json.gz", "rt", encoding="utf-8") as f:
            frames.append(pbp_reshaper(json.load(f), season=2025, game_id=gid))
    return pl.concat(frames, how="diagonal_relaxed")


def test_pbp_parity_vs_frozen_r_oracle():
    py = _build_py()
    oracle_games = pl.read_parquet(ORACLE, columns=["game_id"])["game_id"].unique().sort()
    assert oracle_games.to_list() == list(GAMES)
    sample = [c for c in pl.read_parquet_schema(ORACLE) if c not in KEYS]
    assert_parquet_parity(
        py,
        ORACLE,
        keys=KEYS,
        sample_cols=sample,
        py_only_additive=NAME_COLS,
        dtype_upgrades={"id": (pl.Int64, pl.Float64)},
        # Known divergence, not asserted: sdv-py orders lag_*/lead_* and the
        # coordinate/score_source block differently from hoopR's select.
        require_order=False,
    )


def _raw_plays(gid: int) -> list[dict]:
    with gzip.open(FIXTURE / "raw" / f"{gid}.json.gz", "rt", encoding="utf-8") as f:
        return json.load(f)["plays"]


def test_pbp_play_ids_exact_vs_raw():
    # The R comparison runs through Float64, where distinct 18-digit ids collide;
    # check the exact Int64 ids against the payload, in play order.
    py = _build_py()
    for gid in GAMES:
        ids = py.filter(pl.col("game_id") == gid).sort("game_play_number")["id"].to_list()
        assert ids == [int(p["id"]) for p in _raw_plays(gid)], gid


def test_pbp_athlete_names_resolve():
    # NAME_COLS are excluded from the R comparison, so guard them here: every
    # id-bearing play in these four games resolves a name.
    py = _build_py()
    assert set(NAME_COLS) <= set(py.columns)
    for n in (1, 2):  # hoopR/sdv-py emit athlete_id_1/_2 only
        with_id = py.filter(pl.col(f"athlete_id_{n}").is_not_null())
        assert with_id.filter(pl.col(f"athlete_name_{n}").is_null()).height == 0, n
