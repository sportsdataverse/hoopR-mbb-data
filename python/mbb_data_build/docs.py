"""Generate the per-dataset documentation (spec D40/D43).

Answers, for every dataset: **what builds it, where it is published, what is
in it, and when it last ran.** Ported from the shipped
``hoopR-nba-stats-data/python/nba_data_build/docs.py`` pattern, scoped down
to what this repo actually has:

* This package has no ``models.py`` (pydantic schema declarations) and no
  ``column_descriptions.yaml`` yet -- both are their own deferred phase
  (spec D39 / column-description authoring), not something to fabricate here.
  Column tables are therefore derived from a REAL committed parquet found
  under ``mbb/**/parquet/`` rather than a hand-declared schema, and every
  description cell is empty (an honest TODO, not an invented sentence --
  same philosophy as the reference, just with the whole store still unwritten).
* ``config.REGISTRY`` (15 datasets, ``mbb_data_build.config``) drives PAGES
  directly -- there is no separate ``reshape.datasets`` module here.
* No stage-99 schedule master: ``config.py`` documents that MBB has no
  master-schedule / games-in-repo artifact (unlike NBA), so there is no
  MASTERS section.

**Not wired into the CI drift gate.** ``tests.yml`` sparse-checks out
``python tests scripts R pyproject.toml uv.lock`` -- no ``mbb/`` -- so a
``--check`` run in CI would compare real locally-generated column tables
against permanently-placeholder ones and redden on every push. Regenerate
by hand (``uv run python -m mbb_data_build.docs``) after a schema or
registry change until a small committed schema fixture makes offline
``--check`` meaningful.

Example:
    Regenerate everything::

        uv run python -m mbb_data_build.docs

    Fail if anything is stale (run locally, where mbb/ is checked out)::

        uv run python -m mbb_data_build.docs --check --no-live
"""

from __future__ import annotations

import argparse
import json
import subprocess
from functools import lru_cache
from pathlib import Path

import polars as pl

from mbb_data_build.config import REGISTRY, DatasetSpec

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = REPO_ROOT / "docs" / "datasets"
RELEASE_REPO = "sportsdataverse/sportsdataverse-data"
RELEASE_URL = f"https://github.com/{RELEASE_REPO}/releases/tag"

BEGIN = "<!-- BEGIN GENERATED: datasets -->"
END = "<!-- END GENERATED: datasets -->"

#: dataset key -> the numbered creation script that builds it (build order,
#: not necessarily daily run order -- see CLAUDE.md "Release Tags" table).
BUILDER: dict[str, str] = {
    "pbp": "python/espn_mbb_01_pbp_creation.py",
    "team_box": "python/espn_mbb_02_team_box_creation.py",
    "player_box": "python/espn_mbb_03_player_box_creation.py",
    "rosters": "python/espn_mbb_04_rosters_creation.py",
    "player_season_stats": "python/espn_mbb_05_player_season_stats_creation.py",
    "team_season_stats": "python/espn_mbb_06_team_season_stats_creation.py",
    "standings": "python/espn_mbb_07_standings_creation.py",
    "game_rosters": "python/espn_mbb_09_game_rosters_creation.py",
    "officials": "python/espn_mbb_10_officials_creation.py",
    # Crosswalks: name the builder that ACTUALLY runs. 11 still builds in R in
    # both language modes (KenPom is a paid feed), so its numbered python shim
    # exists but is unwired.
    "team_crosswalk": "R/mbb_11_team_crosswalk_creation.R",
    "schedule_crosswalk": "python/espn_mbb_12_schedule_crosswalk_creation.py",
    "player_crosswalk": "python/espn_mbb_13_player_crosswalk_creation.py",
    "schedules": "python/espn_mbb_14_schedules_creation.py",
    "shots": "python/espn_mbb_15_shots_creation.py",
    "player_core": "python/espn_mbb_16_player_core_creation.py",
}

#: Every documented page: exactly the registry's datasets.
PAGES: tuple[str, ...] = tuple(REGISTRY)

AUTOMATION = (
    "`.github/workflows/daily_mbb.yml` -- daily cron, running "
    "`scripts/daily_mbb_data_processor.sh` (the single entrypoint). "
    "`-l python` is the default and builds via `mbb_data_build`; `-l R` is "
    "the retained rollback path over the R creation scripts."
)

assert set(BUILDER) == set(REGISTRY), "BUILDER must cover exactly the registry's datasets"


def _parquets(spec: DatasetSpec) -> list[Path]:
    """Every committed ``{stem}_{season}.parquet`` on disk, oldest path first.

    Recursive glob rather than ``mbb/{spec.dataset}/parquet/`` because the R
    crosswalk scripts commit under a shared ``mbb/crosswalk/`` directory that
    doesn't match ``spec.dataset`` -- a pre-existing R/Python path divergence
    this module works around rather than papers over.

    The season is matched as FOUR DIGITS, not ``*``: ``schedules``' stem
    (``mbb_schedule``) is a prefix of the schedule crosswalk's
    (``mbb_schedule_crosswalk``), so a bare ``*`` made
    ``mbb_schedule_crosswalk_2026.parquet`` render as a ``crosswalk_2026``
    season of the schedules dataset.
    """
    return sorted(REPO_ROOT.glob(f"mbb/**/parquet/{spec.stem}_[0-9][0-9][0-9][0-9].parquet"))


def _latest_parquet(spec: DatasetSpec) -> Path | None:
    """The newest committed ``{stem}_{season}.parquet`` on disk, if any."""
    hits = _parquets(spec)
    return hits[-1] if hits else None


@lru_cache(maxsize=None)
def _schema(dataset: str) -> pl.Schema | None:
    spec = REGISTRY[dataset]
    path = _latest_parquet(spec)
    if path is None:
        return None
    try:
        return pl.scan_parquet(path).collect_schema()
    except Exception:
        return None


def release_status(tag: str, *, live: bool) -> dict[str, str]:
    """Last-published info for a release tag. Empty when offline or missing."""
    if not live:
        return {}
    try:
        out = subprocess.run(
            ["gh", "release", "view", tag, "--repo", RELEASE_REPO, "--json", "publishedAt,assets"],
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
        if out.returncode != 0:
            return {}
        data = json.loads(out.stdout)
        assets = data.get("assets") or []
        updated = max((a.get("updatedAt") or "" for a in assets), default="")
        return {
            "published": updated[:10],
            "created": (data.get("publishedAt") or "")[:10],
            "assets": str(len(assets)),
        }
    except Exception:
        return {}


def column_table(dataset: str) -> str:
    """The ``col_name | type | description`` table for one dataset.

    Descriptions are always blank -- ``column_descriptions.yaml`` doesn't
    exist yet for this package (spec D39/D40 authoring is separate follow-up
    work); an empty cell here is the honest state, not a placeholder bug.
    """
    schema = _schema(dataset)
    if schema is None:
        return "_No committed parquet found locally to derive a schema from._\n"
    lines = ["| col_name | type | description |", "|---|---|---|"]
    for name, dtype in schema.items():
        lines.append(f"| `{name}` | {dtype} | |")
    return "\n".join(lines) + "\n"


def coverage_table(dataset: str) -> str:
    """Per-season row counts, read straight off the committed parquet tree."""
    spec = REGISTRY[dataset]
    hits = _parquets(spec)
    if not hits:
        return (
            f"_Coverage is tracked per release asset on "
            f"[`{spec.tag}`]({RELEASE_URL}/{spec.tag})._\n"
        )
    lines = ["| season | rows |", "|---:|---:|"]
    for path in hits:
        season = path.stem.removeprefix(f"{spec.stem}_")
        try:
            rows = pl.scan_parquet(path).select(pl.len()).collect().item()
        except Exception:
            rows = "?"
        lines.append(
            f"| {season} | {rows:,} |" if isinstance(rows, int) else f"| {season} | {rows} |"
        )
    return "\n".join(lines) + "\n"


def dataset_page(dataset: str, *, live: bool) -> str:
    spec = REGISTRY[dataset]
    status = release_status(spec.tag, live=live)
    return f"""# `{dataset}`

`{spec.reshaper}` reshaper -- release tag [`{spec.tag}`]({RELEASE_URL}/{spec.tag}).

| | |
|---|---|
| **Builder** | [`{BUILDER[dataset]}`]({"../../" + BUILDER[dataset]}) |
| **Release tag** | [`{spec.tag}`]({RELEASE_URL}/{spec.tag}) |
| **File stem** | `{spec.stem}_{{season}}.{{parquet,csv,rds}}` |
| **Manifested** | {"yes" if spec.manifest_endpoint else "no (no load_mbb_*_manifest() loader yet)"} |
| **Last published** | {status.get("published") or "—"} (newest release asset) |
| **Tag created** | {status.get("created") or "—"} |
| **Release assets** | {status.get("assets") or "—"} |

## Automation

{AUTOMATION}

## Columns

{column_table(dataset)}
## Coverage

{coverage_table(dataset)}"""


def summary_table(*, live: bool) -> str:
    """The block embedded in README.md and CLAUDE.md."""
    lines = [
        "| Script | Dataset | Release tag | Last published |",
        "|---|---|---|---|",
    ]
    for dataset in sorted(PAGES, key=lambda k: BUILDER[k]):
        spec = REGISTRY[dataset]
        builder = BUILDER[dataset]
        status = release_status(spec.tag, live=live)
        lines.append(
            f"| [`{builder}`]({builder}) "
            f"| [`{dataset}`](docs/datasets/{dataset}.md) "
            f"| [`{spec.tag}`]({RELEASE_URL}/{spec.tag}) "
            f"| {status.get('published', '—')} |"
        )
    return "\n".join(lines)


#: Lines whose values move on every publish/data commit; the drift comparison
#: ignores them so a daily run cannot red an unrelated PR.
_VOLATILE = ("**Last published**", "**Tag created**", "**Release assets**")


def _without_status(text: str) -> str:
    kept: list[str] = []
    for line in text.splitlines():
        if any(marker in line for marker in _VOLATILE):
            continue
        if line.startswith("| [`") and line.count("|") >= 5:
            line = "|".join(line.split("|")[:-2]) + "|"
        kept.append(line)
    return "\n".join(kept)


def _replace_block(text: str, block: str) -> str:
    if BEGIN not in text or END not in text:
        return text.rstrip() + f"\n\n## Datasets\n\n{BEGIN}\n{block}\n{END}\n"
    head, _, rest = text.partition(BEGIN)
    _, _, tail = rest.partition(END)
    return f"{head}{BEGIN}\n{block}\n{END}{tail}"


def generate(*, check: bool = False, live: bool = True) -> int:
    """Write (or verify) every generated doc. Returns 0 when in sync."""
    stale: list[str] = []
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    wanted: dict[Path, str] = {DOCS_DIR / f"{d}.md": dataset_page(d, live=live) for d in PAGES}
    block = summary_table(live=live)
    for name in ("README.md", "CLAUDE.md"):
        path = REPO_ROOT / name
        if path.exists():
            wanted[path] = _replace_block(path.read_text(encoding="utf-8"), block)

    for path, content in wanted.items():
        raw = path.read_bytes() if path.exists() else None
        current = raw.decode("utf-8") if raw is not None else None
        # README.md/CLAUDE.md are pre-existing CRLF files on this repo; match
        # whatever line ending the file already has so the diff is the
        # generated block, not every line in the file.
        if raw is not None and b"\r\n" in raw:
            content = content.replace("\n", "\r\n")
        if current == content:
            continue
        if check:
            if current is not None and _without_status(current) == _without_status(content):
                continue
            stale.append(str(path.relative_to(REPO_ROOT)))
        else:
            path.write_text(content, encoding="utf-8", newline="")

    if check and stale:
        print("::error ::generated docs are stale; run `uv run python -m mbb_data_build.docs`")
        for item in stale:
            print(f"  {item}")
        return 1
    if not check:
        print(f"wrote {len(wanted)} generated file(s)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate per-dataset documentation.")
    parser.add_argument("--check", action="store_true", help="Fail if anything is stale")
    parser.add_argument(
        "--no-live",
        action="store_true",
        help="Skip `gh release view` (offline; status columns render as em dashes)",
    )
    args = parser.parse_args(argv)
    return generate(check=args.check, live=not args.no_live)


if __name__ == "__main__":
    raise SystemExit(main())
