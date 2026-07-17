#!/usr/bin/env Rscript
# Serialize the Python-built parquet datasets to .rds and upload rds-only.
#
# R does ZERO reshaping here: the published .rds is byte-derived from the
# parity-passed parquet, so hoopR::load_mbb_* keeps its rds contract while
# Python (mbb_data_build) owns the reshape and publishes the parquet/csv
# assets itself. Ported from the hoopR-nba-data sibling.
#
# WHY THIS EXISTS: hoopR::load_mbb_* reads .rds EXCLUSIVELY (11 rds_from_url
# call sites in hoopR/R/load_mbb.R, zero .parquet references). The python
# cutover moved the datasets to mbb_data_build but did not carry the rds half
# of the contract, so the daily flow published fresh parquet/csv while the rds
# -- the only format the R package actually reads -- silently froze. Every
# hoopR::load_mbb_* user was served stale data while the release looked
# updated. Do not drop this script from the daily flow.
#
# MBB deltas vs the NBA sibling: 12 datasets -- no draft (college), and no
# schedule extras / master schedule (see hoopR-mbb-data/CLAUDE.md).
#
# Usage: Rscript R/serialize_rds.R -s 2026 -e 2026 [--no-upload] [--dataset X]
suppressPackageStartupMessages({
  library(arrow)
  library(glue)
  library(optparse)
  library(purrr)
})

option_list <- list(
  make_option(
    c("-s", "--start_year"),
    action = "store",
    default = hoopR::most_recent_mbb_season(),
    type = "integer"
  ),
  make_option(
    c("-e", "--end_year"),
    action = "store",
    default = hoopR::most_recent_mbb_season(),
    type = "integer"
  ),
  make_option(
    "--no-upload",
    action = "store_true",
    default = FALSE,
    dest = "no_upload",
    help = "serialize locally, skip the release upload"
  ),
  make_option(
    "--dataset",
    action = "store",
    default = "all",
    type = "character",
    help = "serialize a single dataset (e.g. 'player_core') instead of all 12."
  )
)
opt <- parse_args(OptionParser(option_list = option_list))

retry_rate <- purrr::rate_backoff(pause_base = 1, pause_min = 1, max_times = 5)
any_failed <- FALSE

save_rds <- function(df, stem, tag, ds, pkg_fn, out_path) {
  df <- hoopR:::make_hoopR_data(
    df,
    glue("ESPN MBB {ds} from hoopR data repository"),
    Sys.time()
  )
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  saveRDS(df, out_path)
  if (!opt$no_upload) {
    purrr::insistently(
      sportsdataversedata::sportsdataverse_save,
      rate = retry_rate,
      quiet = FALSE
    )(
      data_frame = df,
      file_name = stem,
      sportsdataverse_type = glue("{ds} data"),
      release_tag = tag,
      pkg_function = pkg_fn,
      file_types = c("rds"),
      .token = Sys.getenv("GITHUB_PAT")
    )
  }
  invisible(TRUE)
}

# dataset dir | file stem | release tag | pkg_function
# Mirrors mbb_data_build.config.REGISTRY exactly (tags are load-bearing).
T_ <- "espn_mens_college_basketball_"
DATASETS <- list(
  list("pbp",                 "play_by_play",        paste0(T_, "pbp"),                 "hoopR::load_mbb_pbp()"),
  list("schedules",           "mbb_schedule",        paste0(T_, "schedules"),           "hoopR::load_mbb_schedule()"),
  list("shots",               "shots",               paste0(T_, "shots"),               "hoopR::load_mbb_pbp()"),
  list("team_box",            "team_box",            paste0(T_, "team_boxscores"),      "hoopR::load_mbb_team_box()"),
  list("player_box",          "player_box",          paste0(T_, "player_boxscores"),    "hoopR::load_mbb_player_box()"),
  list("rosters",             "rosters",             paste0(T_, "rosters"),             "hoopR::load_mbb_rosters()"),
  list("player_season_stats", "player_season_stats", paste0(T_, "player_season_stats"), "hoopR::load_mbb_player_stats()"),
  list("player_core",         "player_core",         paste0(T_, "player_core"),         "hoopR::load_mbb_player_core()"),
  list("team_season_stats",   "team_season_stats",   paste0(T_, "team_season_stats"),   "hoopR::load_mbb_team_stats()"),
  list("standings",           "standings",           paste0(T_, "standings"),           "hoopR::load_mbb_standings()"),
  list("game_rosters",        "game_rosters",        paste0(T_, "game_rosters"),        "hoopR::load_mbb_game_rosters()"),
  list("officials",           "officials",           paste0(T_, "officials"),           "hoopR::load_mbb_officials()")
)

if (opt$dataset != "all") {
  keep <- purrr::keep(DATASETS, ~ .x[[1]] == opt$dataset)
  if (length(keep) == 0) {
    stop(glue("unknown --dataset '{opt$dataset}'"))
  }
  DATASETS <- keep
}

for (y in opt$s:opt$e) {
  for (d in DATASETS) {
    ds <- d[[1]]
    stem <- d[[2]]
    tag <- d[[3]]
    pkg_fn <- d[[4]]
    pq <- glue("mbb/{ds}/parquet/{stem}_{y}.parquet")
    if (!file.exists(pq)) {
      cli::cli_alert_info("{Sys.time()}: no parquet for {ds} {y}; skipping rds")
      next
    }
    ok <- tryCatch(
      {
        save_rds(
          arrow::read_parquet(pq),
          glue("{stem}_{y}"),
          tag,
          ds,
          pkg_fn,
          glue("mbb/{ds}/rds/{stem}_{y}.rds")
        )
      },
      error = function(e) {
        cli::cli_alert_warning(
          "{Sys.time()}: rds serialize failed for {ds} {y}: {e$message}"
        )
        FALSE
      }
    )
    if (!ok) any_failed <- TRUE
  }
}

if (any_failed) quit(status = 1)
