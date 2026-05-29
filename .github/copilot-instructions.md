# hoopR-mbb-data Copilot Instructions

## Project Context

This repo is the R-side parser stage for ESPN men's college basketball
data. It reads raw per-game JSON committed to
[`hoopR-mbb-raw`](https://github.com/sportsdataverse/hoopR-mbb-raw),
compiles per-season parquet/RDS frames under `mbb/{pbp,team_box,player_box,schedules}/`,
commits them back to this repo, and uploads them as release assets to
[`sportsdataverse/sportsdataverse-data`](https://github.com/sportsdataverse/sportsdataverse-data)
via `piggyback`. The downstream `hoopR` R package's `load_mbb_*()` helpers
read from those release assets.

Pipeline: `ESPN -> hoopR-mbb-raw -> hoopR-mbb-data [HERE] -> sportsdataverse-data releases -> hoopR`.

- **DESCRIPTION package name**: `hoopR.mbb` (release-asset producer; not installed as a package)
- **License**: CC BY 4.0
- **Coverage**: 2003-present
- **R Requirement**: R >= 4.0.0

## Repository Workflow

- Default and release branch is `main`. Commit directly to `main` — no
  feature branches unless an explicit cleanup PR is requested.
- The CI entry point is `bash scripts/daily_mbb_R_processor.sh -s <START> -e <END>`.
- Daily commits use the load-bearing format
  `MBB Data Update (Start: YYYY End: YYYY)`. The downstream
  `daily_mbb_data` dispatch grep-extracts the year digits — do not
  change this format casually.
- ESPN parsing bugs belong in `hoopR-mbb-raw` (or upstream in `hoopR` /
  `sportsdataverse-py`), not here. This repo is a thin compile + upload.

## Build & Development Commands

```sh
# Full daily flow (per-year loop, commit + push after each season)
bash scripts/daily_mbb_R_processor.sh -s 2025 -e 2025

# Or call each creation script directly when iterating
Rscript R/espn_mbb_01_pbp_creation.R        -s 2025 -e 2025
Rscript R/espn_mbb_02_team_box_creation.R   -s 2025 -e 2025
Rscript R/espn_mbb_03_player_box_creation.R -s 2025 -e 2025

# Helpers (interactive / one-off — not run by CI)
Rscript R/rebuild_mbb_master_schedule.R
Rscript R/minify_json_folders.R
Rscript R/0000_create_hoopR_releases_init.R
Rscript R/0001_push_existing_release_data.R
```

Output paths the creation scripts write under (committed back to this
repo each run):

- `mbb/pbp/{rds,parquet}/play_by_play_{year}.{ext}`
- `mbb/team_box/{rds,parquet}/team_box_{year}.{ext}`
- `mbb/player_box/{rds,parquet}/player_box_{year}.{ext}`
- `mbb/schedules/{rds,parquet}/mbb_schedule_{year}.{ext}`

## Release Tags (sportsdataverse-data)

| Script | Release tag |
|--------|-------------|
| `espn_mbb_01_pbp_creation.R` | `espn_mens_college_basketball_pbp` |
| `espn_mbb_02_team_box_creation.R` | `espn_mens_college_basketball_team_boxscores` |
| `espn_mbb_03_player_box_creation.R` | `espn_mens_college_basketball_player_boxscores` |
| `espn_mbb_03_player_box_creation.R` (also) | `espn_mens_college_basketball_schedules` |

The tag strings are read directly by `hoopR::load_mbb_*()` — renaming a
tag here breaks downstream URL construction.

## Code Style

- Tidyverse style: `snake_case`, 2-space indent, `%>%` pipe.
- Use `purrr::map_dfr` + `furrr::future_map_dfr` for per-game compile;
  `future::plan("multisession")` before the parallel call.
- Use `data.table::fwrite()` for CSV writes; `arrow::write_parquet()` and
  `saveRDS()` for the canonical per-season outputs.
- Read raw payloads from `hoopR-mbb-raw` via the `raw.githubusercontent.com`
  URLs already used in `R/espn_mbb_0[1-3]_*.R`. Do not re-scrape ESPN here.
- `hoopR:::most_recent_mbb_season()`, `hoopR:::rds_from_url()`, and
  `hoopR:::espn_mbb_pbp()` are the canonical helpers — call them; don't
  reinvent them.
- Keep DESCRIPTION's `Imports:` / `Remotes:` consistent with what the
  scripts actually load. The CI runner uses
  `r-lib/actions/setup-r-dependencies@v2` to install them.

## Daily Workflow

`.github/workflows/daily_mbb.yml` is the in-repo cron entry point.

- **Cron**: `0 7 UTC` daily, gated to MBB season (late Oct, Nov-Dec,
  Jan-Mar, all April). Sits 2 hours after the raw repo's `0 5 UTC`
  scrape so the raw payload is on `main` before this parser pulls.
- **`repository_dispatch`**: event-type `daily_mbb_data`, fired from
  `hoopR-mbb-raw`'s push trigger. The client_payload commit_message
  carries the year range as digits.
- **`workflow_dispatch`**: optional `start_year` / `end_year` inputs;
  defaults to `hoopR::most_recent_mbb_season()` when blank.
- **Runner**: `windows-latest` (intentional — historical maintainer setup).
- **Secrets**:
  - `SDV_GH_TOKEN` — cross-repo `piggyback::pb_upload()` to
    `sportsdataverse-data`.
  - `GITHUB_TOKEN` — in-repo commit/push.

## Project-Specific Gotchas

- **Windows runner** means avoid case-sensitive paths and shell-isms;
  use `file.path()` and lowercase paths.
- **`R/rebuild_mbb_master_schedule.R`** has the maintainer's local
  `.libPaths()` hardcoded — do not run from CI.
- The 2003-2008 ESPN payloads have many empty per-game JSONs. The
  pbp/team_box/player_box compiles silently emit an empty per-game frame
  in that case — do not refactor to error on empty.
- The repo holds GBs of `.rds` + `.parquet` per-season frames under
  `mbb/`. Reorganizing this tree without updating both `hoopR::load_mbb_*()`
  and the upstream raw repo's path expectations will break the pipeline.

## Cross-Repo References

- Upstream raw scraper: <https://github.com/sportsdataverse/hoopR-mbb-raw>
- Release-asset host: <https://github.com/sportsdataverse/sportsdataverse-data>
- Downstream R package: <https://github.com/sportsdataverse/hoopR>
- Sister data repos: hoopR-nba-data, wehoop-wnba-data, wehoop-wbb-data

## Conventional Commits

Use: `type(scope): description`. Common types: `feat`, `fix`, `chore`,
`ci`, `docs`, `refactor`. Common scopes: `pbp`, `team_box`, `player_box`,
`schedules`, `release`, `ci`, `deps`. Use `type!:` or a `BREAKING CHANGE:`
footer for breaking changes.

**The exception**: daily data-update commits use the load-bearing format
`MBB Data Update (Start: YYYY End: YYYY)`. Do not switch those to
conventional-commits — the downstream `daily_mbb_data` dispatch parses
the year digits from this string.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
