"""Join-key id dtypes in what the producer writes.

Every dataset carries game/team/athlete ids as Int32, except that sdv-py's
game_rosters helper emits ``game_id`` as a string and its rosters helper emits
``athlete_id`` as a string. ``io.write_dataset`` casts them, so
``game_rosters`` joins ``schedules`` on ``game_id`` and ``rosters`` joins
``player_box`` on ``athlete_id``.
"""

import gzip
import json
from pathlib import Path

import polars as pl
import pytest
from mbb_data_build import io
from mbb_data_build.config import REGISTRY
from sportsdataverse.mbb import helper_mbb_game_rosters

RAW = Path(__file__).resolve().parents[1] / "fixtures" / "pbp_r_oracle" / "raw"


def _written(df: pl.DataFrame, dataset: str, tmp_path: Path) -> dict:
    spec = REGISTRY[dataset]
    io.write_dataset(df, spec, 2025, base=tmp_path)
    return dict(
        pl.read_parquet_schema(tmp_path / dataset / "parquet" / f"{spec.stem}_2025.parquet")
    )


def test_game_rosters_ids_are_int32_from_a_real_payload(tmp_path):
    with gzip.open(RAW / "401721734.json.gz", "rt", encoding="utf-8") as f:
        df = helper_mbb_game_rosters(json.load(f), season=2025, game_id=401721734)
    schema = _written(df, "game_rosters", tmp_path)
    assert {c: schema[c] for c in ("game_id", "team_id", "athlete_id")} == dict.fromkeys(
        ("game_id", "team_id", "athlete_id"), pl.Int32
    )


def test_rosters_athlete_id_is_int32(tmp_path):
    df = pl.DataFrame(
        {"team_id": [2, 2, 2], "athlete_id": ["5341547", None, "4433218"]},
        schema={"team_id": pl.Int32, "athlete_id": pl.String},
    )
    schema = _written(df, "rosters", tmp_path)
    assert schema["athlete_id"] == pl.Int32
    assert schema["team_id"] == pl.Int32
    # Values survive the cast and a null stays null.
    back = pl.read_parquet(tmp_path / "rosters" / "parquet" / "rosters_2025.parquet")
    assert back["athlete_id"].to_list() == [5341547, None, 4433218]


def test_non_numeric_id_fails_the_write(tmp_path):
    df = pl.DataFrame({"game_id": ["401721734", "not-an-id"]})
    with pytest.raises(pl.exceptions.InvalidOperationError):
        io.write_dataset(df, REGISTRY["game_rosters"], 2025, base=tmp_path)
