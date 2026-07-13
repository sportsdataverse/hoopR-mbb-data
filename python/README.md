# mbb_data_build — Python season-assembly build

A polars port of the `R/espn_mbb_*_creation.R` compile. It reshapes the sibling
[`hoopR-mbb-raw`](https://github.com/sportsdataverse/hoopR-mbb-raw) per-game JSON
into season-level parquet/csv + a manifest and (optionally) uploads them to the
`espn_mens_college_basketball_*` release tags on
[`sportsdataverse-data`](https://github.com/sportsdataverse/sportsdataverse-data).

**Every reshape delegates to a `sportsdataverse.mbb` release producer** — this
package is the season-assembly loop + I/O + publish glue, not the parsing. The R
scripts remain the byte-parity oracle.

```
hoopR-mbb-raw ──(per-game JSON)──▶ mbb_data_build ──(release upload)──▶ sportsdataverse-data ──▶ hoopR::load_mbb_*()
```

This is a **parallel implementation** — the R pipeline (`scripts/daily_mbb_R_processor.sh`)
still drives daily CI. Wiring this package into CI is a separate cutover.

## Run

```sh
# from python/ ; needs uv (https://docs.astral.sh/uv/)
uv sync
uv run python -m mbb_data_build --dataset pbp -s 2025 -e 2025            # build one dataset locally
uv run python -m mbb_data_build --dataset officials -s 2025 -e 2025 --dry-run   # build + show what would publish
uv run python -m mbb_data_build --dataset officials -s 2025 -e 2025 --publish   # build + upload release assets
uv run pytest -q                                                        # full-frame parity vs the committed R oracles
```

- `--dataset` — one of the registry keys (`pbp`, `team_box`, `player_box`,
  `schedules`, `shots`, `rosters`, `player_season_stats`, `team_season_stats`,
  `standings`, `game_rosters`, `officials`; **no draft** — MBB has none).
- `-s`/`-e` — inclusive season range (end year: `2025` = the 2024-25 season).
- `--raw-root` — sibling `hoopR-mbb-raw` checkout, or an HTTP base URL
  (`HOOPR_MBB_RAW_ROOT` env is the default; a base URL reads over HTTP).
- `--publish` uploads per-file with `--clobber` (never delete-then-recreate,
  which would open a 404 window for live loaders). `--dry-run` builds without
  uploading.

## Dependency

`sportsdataverse` is pinned to git `main` in `pyproject.toml` (`[tool.uv.sources]`)
— the `helper_mbb_*` producers merged there in sdv-py #256.

## Parity

`tests/mbb_data_build/test_parity_*.py` build each dataset for a real season into
a temp dir and assert full-frame equality against the committed R oracle parquet
under `mbb/<dataset>/parquet/`. The build never writes into the committed `mbb/`
tree during tests.

Notes on two deliberate divergences the tests document:
- `player_season_stats` iterates the identity-lookup athlete ids (R's
  `names(identity_lookup)`), and excludes two athlete ids whose *oracle* identity
  disagrees with its own source payload (guarded so the set can't silently grow).
- `officials` and `game_rosters` fall back to `json/final` when the recent-only
  `game_rosters/json` sidecar is absent, recovering pre-scrape seasons with zero
  HTTP. `officials`' `gameInfo.officials` block is byte-identical there;
  `game_rosters`' boxscore roster is structurally identical, diverging only on
  occasional display-name formatting ("L.J. Cryer" vs "LJ Cryer"), and historical
  seasons have no `game_rosters/json` oracle so `json/final` is authoritative.
  The fallback is purely additive — current seasons keep using `game_rosters/json`.
