"""Parity: rosters / team_season_stats / player_season_stats / standings vs
the committed 2025 release parquet, FULL 2025 season.

The oracle is this repo's published ``mbb/`` tree, written by whichever chain last
published it. As of 2026-11-26 these four 2025 files are R-written (arrow footer with an
``r`` key); a Python republish would turn the comparison into a Python-vs-release
regression check, so ``_oracle_kind`` reads the footer and failures say which one it is.
The R port of the #1201 split is also checked offline in
``tests/test_split_made_attempted_r.py``.

Port provenance: the script-local parsers in
``hoopR-mbb-data/R/espn_mbb_0{4,5,6,7}_*_creation.R``. ``player_season_stats``
is the MBB/NBA-shared builder (identity backfilled from the season's built
player_box, not team rosters -- see ``reshapers.player_season_stats_builder``);
the other three delegate to the shared WBB implementation after league
normalization. Long-format frames have no compact unique key, so ALL columns
act as sort keys (total order; duplicate rows compare as multisets).
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest
from mbb_data_build import ingest

from tests.mbb_data_build._parity_helpers import assert_parquet_parity
from tests.mbb_data_build.conftest import RAW_ROOT, oracle_path

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


def _oracle_kind(path) -> str:
    """What a comparison against ``path`` checks, from the writer recorded in its footer."""
    if "r" in pl.read_parquet_metadata(path):
        return "R parity (oracle written by the R chain)"
    return "Python-vs-release regression (oracle is not R-written)"


def _pinned_ids(name: str) -> set[int]:
    lines = (_FIXTURES / name).read_text(encoding="utf-8").splitlines()
    return {int(x) for x in (ln.strip() for ln in lines) if x and not x.startswith("#")}


@pytest.mark.parametrize(
    ("dataset", "stem", "keys"),
    [
        ("rosters", "rosters", ["team_id", "athlete_id"]),
        ("team_season_stats", "team_season_stats", None),
        ("standings", "standings", None),
    ],
)
def test_season_dataset_parity_full_2025(dataset, stem, keys, built_base):
    py = pl.read_parquet(built_base / dataset / "parquet" / f"{stem}_2025.parquet")
    oracle = oracle_path(dataset, stem)
    all_cols = list(pl.read_parquet_schema(str(oracle)))
    keys = keys if keys is not None else all_cols
    sample = [c for c in all_cols if c not in keys]
    # The producer writes rosters.athlete_id as Int32 (io.write_dataset). A committed
    # season not yet republished still carries String: pin that pair. Once it
    # is republished as Int32, no upgrade applies and dtypes must match exactly.
    legacy = dataset == "rosters" and pl.read_parquet_schema(str(oracle))["athlete_id"] == pl.String
    upgrades = {"athlete_id": (pl.Int32, pl.String)} if legacy else None
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample, dtype_upgrades=upgrades)


# Identity columns. The released 2025 file predates two upstream changes, so the
# fresh build is checked against their sources instead of against the release:
# * jersey / position / team_display_name come from the athlete's latest-dated
#   player_box row (sportsdataverse-py #1194; R's build_identity_lookup sorts the
#   same way). The release kept a frame-order pick for 108 athletes.
# * team_slug comes from the raw season-stats payload, which ESPN has since
#   re-slugged for renamed schools (e.g. saint-francis-red-flash -> -red-wolves).
# The parity test excludes these columns only for those stale athletes; every other
# column, and these for every other athlete, must match the release exactly.
_IDENTITY_FROM_PLAYER_BOX = ("athlete_jersey", "athlete_position_abbreviation", "team_display_name")
_IDENTITY_COLS = (*_IDENTITY_FROM_PLAYER_BOX, "team_slug")
_IDENTITY_KEY = ["athlete_id", "athlete_display_name", *_IDENTITY_FROM_PLAYER_BOX]


def _split_count_names(df: pl.DataFrame) -> list[str]:
    """Stat names of the numeric count rows sportsdataverse-py #1201 derives.

    Each ``fooMade-fooAttempted`` text row is followed by numeric ``fooMade``
    and ``fooAttempted`` rows. Releases published before #1201 lack them.
    """
    names = df.get_column("stat_name").drop_nulls().unique().to_list()
    return sorted({part for n in names if n.count("-") == 1 for part in n.split("-")})


def _without_split_counts(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(~pl.col("stat_name").is_in(_split_count_names(df)))


def _built(built_base: Path, dataset: str) -> pl.DataFrame:
    return pl.read_parquet(built_base / dataset / "parquet" / f"{dataset}_2025.parquet")


def test_player_season_stats_split_counts_match_their_text_2025(built_base):
    # Both pipelines emit the #1201 count rows (espn_mbb_05 via split_made_attempted).
    py = _built(built_base, "player_season_stats")
    split_names = _split_count_names(py)
    assert split_names, "expected the #1201 made/attempted count rows"

    derived = py.filter(pl.col("stat_name").is_in(split_names))
    parents = py.filter(pl.col("stat_name").str.count_matches("-") == 1)
    assert derived.height == 2 * parents.height

    # each derived row is its half of the parent's "made-attempted" row: name, count and
    # text (label + "M"/"A", display name with "Made"/"Attempted"; null when empty)
    key = ["athlete_id", "team_id", "category"]
    halves = parents.with_columns(
        _name=pl.col("stat_name").str.split("-"),
        _val=pl.col("display_value").str.split("-"),
    )
    text = ["stat_name", "value", "display_value", "stat_label", "stat_display_name"]
    got = derived.select(*key, *text)
    label = pl.col("stat_label")
    for i, (suffix, word) in enumerate((("M", "Made"), ("A", "Attempted"))):
        half = pl.col("_val").list.get(i)
        dn = pl.col("stat_display_name").str.replace_all("Made-Attempted", word, literal=True)
        expected = halves.select(
            *key,
            stat_name=pl.col("_name").list.get(i),
            value=half.cast(pl.Float64),
            display_value=half,
            stat_label=pl.when(label.is_null() | (label == ""))
            .then(None)
            .otherwise(label + suffix),
            stat_display_name=pl.when(dn == "").then(None).otherwise(dn),
        )
        missing = expected.join(got, on=[*key, *text], how="anti", nulls_equal=True)
        assert missing.is_empty(), f"derived rows disagree with their parent:\n{missing.head()}"


def _latest_player_box_identity(built_base: Path) -> pl.DataFrame:
    # every row on the athlete's latest game date is acceptable: a same-day doubleheader
    # for two teams (5116992, 2024-11-04) has no later game to break the tie.
    pb = _built(built_base, "player_box")
    latest = pb.filter(pl.col("game_date") == pl.col("game_date").max().over("athlete_id"))
    return latest.select(_IDENTITY_KEY).unique()


def _moved_slugs(py: pl.DataFrame, r: pl.DataFrame) -> pl.DataFrame:
    """(athlete_id, team_slug) pairs of the build whose slug the release does not carry."""
    return (
        py.select("athlete_id", "team_slug")
        .unique()
        .join(
            r.select("athlete_id", "team_slug").unique(),
            on=["athlete_id", "team_slug"],
            how="anti",
            nulls_equal=True,
        )
    )


def _raw_payload_slugs(athlete_id: int) -> set:
    payload = ingest.read_final(athlete_id, raw_root=RAW_ROOT, subdir="player_season_stats/json")
    assert payload is not None, athlete_id
    return {
        s.get("teamSlug")
        for cat in payload.get("categories", [])
        for s in cat.get("statistics", [])
    }


# Athletes whose 2025 release identity is stale, computed from a fresh build on 2026-11-26
# and pinned: the observed sets must stay inside them, so a new stale athlete fails even
# when another one drops out. Both shrink to empty once 2025 is republished.
_STALE_IDENTITY_IDS = "pss_2025_stale_identity_ids.txt"
_RESLUGGED_IDS = "pss_2025_reslugged_ids.txt"


def stale_identity_ids(r: pl.DataFrame, latest_identity: pl.DataFrame) -> set[int]:
    """Athletes whose release identity is not a latest-dated player_box identity."""
    stale = (
        r.select(_IDENTITY_KEY)
        .unique()
        .join(latest_identity, on=_IDENTITY_KEY, how="anti", nulls_equal=True)
    )
    return set(stale.get_column("athlete_id").to_list())


def reslugged_ids(py: pl.DataFrame, r: pl.DataFrame) -> set[int]:
    """Athletes whose built team_slug differs from the release and is in the raw payload."""
    return {aid for aid, slug in _moved_slugs(py, r).iter_rows() if slug in _raw_payload_slugs(aid)}


def test_player_season_stats_parity_full_2025(built_base):
    py = _built(built_base, "player_season_stats")
    release = oracle_path("player_season_stats", "player_season_stats")
    r = pl.read_parquet(release)
    assert set(_IDENTITY_COLS) <= set(r.columns)
    kind = _oracle_kind(release)

    # The release predates #1201 until it is republished: only then strip the count rows.
    # Once it carries them, the count rows are compared in full (R's split_made_attempted()
    # output when the release is R-written).
    if r.filter(pl.col("stat_name").is_in(_split_count_names(py))).is_empty():
        py, r = _without_split_counts(py), _without_split_counts(r)
        kind = f"{kind}, #1201 count rows stripped"

    # Only the stale part of a stale athlete is excluded: the player_box identity columns
    # for identity-stale athletes, team_slug for re-slugged ones. Every other athlete is
    # compared on every column.
    stale_identity = stale_identity_ids(r, _latest_player_box_identity(built_base))
    reslugged = reslugged_ids(py, r)
    new = stale_identity - _pinned_ids(_STALE_IDENTITY_IDS)
    assert not new, f"{len(new)} athletes newly identity-stale: {sorted(new)[:20]}"
    new = reslugged - _pinned_ids(_RESLUGGED_IDS)
    assert not new, f"{len(new)} athletes newly re-slugged: {sorted(new)[:20]}"
    masks = [(stale_identity, _IDENTITY_FROM_PLAYER_BOX), (reslugged, ("team_slug",))]

    def mask(df: pl.DataFrame) -> pl.DataFrame:
        for ids, cols in masks:
            stale = pl.col("athlete_id").is_in(sorted(ids))
            df = df.with_columns(
                pl.when(stale).then(None).otherwise(pl.col(c)).alias(c) for c in cols
            )
        return df

    try:
        assert_parquet_parity(
            mask(py),
            # assert_parquet_parity re-reads from a path, so write the masked oracle to a
            # temp file rather than teach the helper a frame input.
            _write_tmp(mask(r)),
            keys=list(r.columns),
            sample_cols=[],
        )
    except AssertionError as e:
        raise AssertionError(f"{kind}: {e}") from e


def test_player_season_stats_identity_is_the_latest_player_box_row_2025(built_base):
    got = _built(built_base, "player_season_stats").select(_IDENTITY_KEY).unique()
    assert got.get_column("athlete_id").is_unique().all(), "one identity per athlete"
    latest = _latest_player_box_identity(built_base)
    off = got.join(latest, on=_IDENTITY_KEY, how="anti", nulls_equal=True)
    assert off.is_empty(), f"identity is not from the latest-dated player_box row:\n{off.head()}"


def test_player_season_stats_team_slug_is_the_raw_payload_slug_2025(built_base):
    py = _built(built_base, "player_season_stats")
    r = pl.read_parquet(oracle_path("player_season_stats", "player_season_stats"))
    moved = _moved_slugs(py, r)
    new = set(moved.get_column("athlete_id").to_list()) - _pinned_ids(_RESLUGGED_IDS)
    assert not new, f"{len(new)} athletes newly changed team_slug: {sorted(new)[:20]}"
    for aid, slug in moved.iter_rows():
        slugs = _raw_payload_slugs(aid)
        assert slug in slugs, f"{aid}: {slug!r} not in its raw payload {sorted(map(str, slugs))}"


def _write_tmp(df: pl.DataFrame) -> Path:
    p = Path(tempfile.mkdtemp(prefix="pss_oracle_")) / "filtered.parquet"
    df.write_parquet(p)
    return p
