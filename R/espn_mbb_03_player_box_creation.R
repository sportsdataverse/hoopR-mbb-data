rm(list = ls())
gcol <- gc()
# lib_path <- Sys.getenv("R_LIBS")
# if (!requireNamespace("pacman", quietly = TRUE)) {
#   install.packages("pacman", lib = Sys.getenv("R_LIBS"), repos = "http://cran.us.r-project.org")
# }
suppressPackageStartupMessages(suppressMessages(library(dplyr)))
suppressPackageStartupMessages(suppressMessages(library(magrittr)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite)))
suppressPackageStartupMessages(suppressMessages(library(purrr)))
suppressPackageStartupMessages(suppressMessages(library(progressr)))
suppressPackageStartupMessages(suppressMessages(library(data.table)))
suppressPackageStartupMessages(suppressMessages(library(arrow)))
suppressPackageStartupMessages(suppressMessages(library(glue)))
suppressPackageStartupMessages(suppressMessages(library(optparse)))

option_list <- list(
  make_option(
    c("-s", "--start_year"),
    action = "store",
    default = hoopR:::most_recent_mbb_season(),
    type = "integer",
    help = "Start year of the seasons to process"
  ),
  make_option(
    c("-e", "--end_year"),
    action = "store",
    default = hoopR:::most_recent_mbb_season(),
    type = "integer",
    help = "End year of the seasons to process"
  )
)
opt <- parse_args(OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- opt$s:opt$e

# --- compile into player_box_{year}.parquet ---------

# hoopR#23: ESPN sometimes lists the same athlete inside BOTH teams'
# boxscore player arrays with an identical stat line (e.g. game 401253901).
# Keep the copy on the athlete's true team, resolved by (1) the row ESPN
# marks starter = TRUE (the foreign copy never is), else (2) the athlete's
# modal team_id across the season's non-duplicated rows (strict majority).
# Pairs neither signal resolves are kept as-is; NA athlete_ids are never
# treated as duplicates of each other. Mirrors
# python/mbb_data_build/reshapers.py::dedupe_player_box_dual_team.
dedupe_player_box_dual_team <- function(df) {
  if (!all(c("game_id", "athlete_id", "team_id") %in% names(df))) {
    return(df)
  }
  df <- df %>%
    dplyr::group_by(.data$game_id, .data$athlete_id) %>%
    dplyr::mutate(
      .dupe = !is.na(.data$athlete_id) & dplyr::n_distinct(.data$team_id) > 1
    ) %>%
    dplyr::ungroup()
  if (!any(df$.dupe)) {
    return(dplyr::select(df, -".dupe"))
  }
  modal <- df %>%
    dplyr::filter(!.data$.dupe) %>%
    dplyr::count(.data$athlete_id, .data$team_id) %>%
    dplyr::group_by(.data$athlete_id) %>%
    dplyr::filter(.data$n == max(.data$n)) %>%
    dplyr::filter(dplyr::n() == 1) %>%
    dplyr::ungroup() %>%
    dplyr::select("athlete_id", .modal_team = "team_id")
  n_before <- nrow(df)
  df <- df %>%
    dplyr::left_join(modal, by = "athlete_id") %>%
    dplyr::mutate(
      .is_starter = .data$starter %in% TRUE,
      .is_modal = !is.na(.data$.modal_team) &
        .data$team_id == .data$.modal_team
    ) %>%
    dplyr::group_by(.data$game_id, .data$athlete_id) %>%
    dplyr::mutate(
      .n_starter = sum(.data$.is_starter & .data$.dupe),
      .n_modal = sum(.data$.is_modal & .data$.dupe)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(
      !.data$.dupe |
        dplyr::case_when(
          .data$.n_starter == 1 ~ .data$.is_starter,
          .data$.n_modal == 1 ~ .data$.is_modal,
          TRUE ~ TRUE
        )
    ) %>%
    dplyr::select(
      -".dupe", -".modal_team", -".is_starter", -".is_modal",
      -".n_starter", -".n_modal"
    )
  if (nrow(df) < n_before) {
    cli::cli_alert_info(
      "player_box: dropped {n_before - nrow(df)} dual-team duplicate rows (hoopR#23)"
    )
  }
  df
}

mbb_player_box_games <- function(y) {
  espn_df <- data.frame()
  sched <- readRDS(paste0("mbb/schedules/rds/mbb_schedule_", y, ".rds"))

  season_player_box_list <- sched %>%
    dplyr::filter(.data$game_json == TRUE) %>%
    dplyr::pull("game_id")

  if (length(season_player_box_list) > 0) {
    cli::cli_progress_step(
      msg = "Compiling {y} ESPN MBB Player Boxscores ({length(season_player_box_list)} games)",
      msg_done = "Compiled {y} ESPN MBB Player Boxscores!"
    )

    future::plan("multisession")
    espn_df <- furrr::future_map_dfr(
      season_player_box_list,
      function(x) {
        tryCatch(
          expr = {
            resp <- glue::glue(
              "https://raw.githubusercontent.com/sportsdataverse/hoopR-mbb-raw/main/mbb/json/final/{x}.json"
            )
            tryCatch(
              hoopR:::helper_espn_mbb_player_box(resp),
              error = function(e) NULL,
              warning = function(w) NULL
            )
          },
          error = function(e) {
            message(glue::glue(
              "{Sys.time()}: Player box score data for {x} issue!"
            ))
          }
        )
      },
      .options = furrr::furrr_options(seed = TRUE)
    )

    cli::cli_progress_step(
      msg = "Updating {y} ESPN MBB Player Boxscores GitHub Release",
      msg_done = "Updated {y} ESPN MBB Player Boxscores GitHub Release!"
    )
  }
  if (nrow(espn_df) > 1) {
    espn_df <- espn_df %>%
      dedupe_player_box_dual_team() %>%
      dplyr::arrange(dplyr::desc(.data$game_date)) %>%
      hoopR:::make_hoopR_data(
        "ESPN MBB Player Boxscores from hoopR data repository",
        Sys.time()
      )

    ifelse(
      !dir.exists(file.path("mbb/player_box")),
      dir.create(file.path("mbb/player_box")),
      FALSE
    )

    # ifelse(!dir.exists(file.path("mbb/player_box/csv")), dir.create(file.path("mbb/player_box/csv")), FALSE)
    # data.table::fwrite(espn_df, file = paste0("mbb/player_box/csv/player_box_", y, ".csv.gz"))

    # ifelse(!dir.exists(file.path("mbb/player_box/qs")), dir.create(file.path("mbb/player_box/qs")), FALSE)
    # qs::qsave(espn_df, glue::glue("mbb/player_box/qs/player_box_{y}.qs"))

    ifelse(
      !dir.exists(file.path("mbb/player_box/rds")),
      dir.create(file.path("mbb/player_box/rds")),
      FALSE
    )
    saveRDS(espn_df, glue::glue("mbb/player_box/rds/player_box_{y}.rds"))

    ifelse(
      !dir.exists(file.path("mbb/player_box/parquet")),
      dir.create(file.path("mbb/player_box/parquet")),
      FALSE
    )
    arrow::write_parquet(
      espn_df,
      glue::glue("mbb/player_box/parquet/player_box_{y}.parquet")
    )

    retry_rate <- purrr::rate_backoff(
      pause_base = 1,
      pause_min = 60,
      max_times = 10
    )
    purrr::insistently(
      sportsdataversedata::sportsdataverse_save,
      rate = retry_rate,
      quiet = FALSE
    )(
      data_frame = espn_df,
      file_name = glue::glue("player_box_{y}"),
      sportsdataverse_type = "player boxscores data",
      release_tag = "espn_mens_college_basketball_player_boxscores",
      pkg_function = "hoopR::load_mbb_player_box()",
      file_types = c("rds", "csv", "parquet"),
      .token = Sys.getenv("GITHUB_PAT")
    )
  }

  sched <- sched %>%
    dplyr::mutate(dplyr::across(
      dplyr::any_of(c(
        "id",
        "game_id",
        "type_id",
        "status_type_id",
        "home_id",
        "home_venue_id",
        "home_conference_id",
        "home_score",
        "away_id",
        "away_venue_id",
        "away_conference_id",
        "away_score",
        "season",
        "season_type",
        "groups_id",
        "tournament_id",
        "venue_id"
      )),
      ~ as.integer(.x)
    )) %>%
    dplyr::mutate(
      status_display_clock = as.character(.data$status_display_clock),
      game_date_time = lubridate::ymd_hm(substr(
        .data$date,
        1,
        nchar(.data$date) - 1
      )) %>%
        lubridate::with_tz(tzone = "America/New_York"),
      game_date = as.Date(substr(.data$game_date_time, 1, 10))
    )

  if (nrow(espn_df) > 0) {
    sched <- sched %>%
      dplyr::mutate(
        player_box = ifelse(
          .data$game_id %in% unique(espn_df$game_id),
          TRUE,
          FALSE
        )
      )
  } else {
    cli::cli_alert_info(
      "{length(season_player_box_list)} ESPN MBB Player Boxscores to be compiled for {y}, skipping Player Boxscores compilation"
    )
    sched$player_box <- FALSE
  }

  final_sched <- sched %>%
    dplyr::distinct() %>%
    dplyr::arrange(dplyr::desc(.data$date))

  final_sched <- final_sched %>%
    hoopR:::make_hoopR_data(
      "ESPN MBB Schedule from hoopR data repository",
      Sys.time()
    )

  retry_rate <- purrr::rate_backoff(
    pause_base = 1,
    pause_min = 60,
    max_times = 10
  )
  purrr::insistently(
    sportsdataversedata::sportsdataverse_save,
    rate = retry_rate,
    quiet = FALSE
  )(
    data_frame = final_sched,
    file_name = glue::glue("mbb_schedule_{y}"),
    sportsdataverse_type = "schedule data",
    release_tag = "espn_mens_college_basketball_schedules",
    pkg_function = "hoopR::load_mbb_schedule()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  ifelse(
    !dir.exists(file.path("mbb/schedules")),
    dir.create(file.path("mbb/schedules")),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("mbb/schedules/rds")),
    dir.create(file.path("mbb/schedules/rds")),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("mbb/schedules/parquet")),
    dir.create(file.path("mbb/schedules/parquet")),
    FALSE
  )
  saveRDS(final_sched, glue::glue("mbb/schedules/rds/mbb_schedule_{y}.rds"))
  arrow::write_parquet(
    final_sched,
    glue::glue("mbb/schedules/parquet/mbb_schedule_{y}.parquet")
  )
  rm(sched)
  rm(final_sched)
  rm(espn_df)
  gc()
  return(NULL)
}

all_games <- purrr::map(years_vec, function(y) {
  mbb_player_box_games(y)
  return(NULL)
})

cli::cli_progress_step(
  msg = "Compiling ESPN MBB master schedule",
  msg_done = "ESPN MBB master schedule compiled and written to disk"
)

sched_list <- list.files(path = glue::glue("mbb/schedules/rds/"))
sched_g <- purrr::map_dfr(sched_list, function(x) {
  sched <- readRDS(paste0("mbb/schedules/rds/", x)) %>%
    dplyr::mutate(dplyr::across(
      dplyr::any_of(c(
        "id",
        "game_id",
        "type_id",
        "status_type_id",
        "home_id",
        "home_venue_id",
        "home_conference_id",
        "home_score",
        "away_id",
        "away_venue_id",
        "away_conference_id",
        "away_score",
        "season",
        "season_type",
        "groups_id",
        "tournament_id",
        "venue_id"
      )),
      ~ as.integer(.x)
    )) %>%
    dplyr::mutate(
      status_display_clock = as.character(.data$status_display_clock),
      game_date_time = lubridate::ymd_hm(substr(
        .data$date,
        1,
        nchar(.data$date) - 1
      )) %>%
        lubridate::with_tz(tzone = "America/New_York"),
      game_date = as.Date(substr(.data$game_date_time, 1, 10))
    )
  return(sched)
})

sched_g <- sched_g %>%
  hoopR:::make_hoopR_data(
    "ESPN MBB Schedule from hoopR data repository",
    Sys.time()
  )

final_sched <- sched_g %>%
  dplyr::arrange(dplyr::desc(.data$date))

retry_rate <- purrr::rate_backoff(
  pause_base = 1,
  pause_min = 60,
  max_times = 10
)
purrr::insistently(
  sportsdataversedata::sportsdataverse_save,
  rate = retry_rate,
  quiet = FALSE
)(
  data_frame = final_sched,
  file_name = glue::glue("mbb_schedule_master"),
  sportsdataverse_type = "schedule data",
  release_tag = "espn_mens_college_basketball_schedules",
  pkg_function = "hoopR::load_mbb_schedule()",
  file_types = c("rds", "csv", "parquet"),
  .token = Sys.getenv("GITHUB_PAT")
)

retry_rate <- purrr::rate_backoff(
  pause_base = 1,
  pause_min = 60,
  max_times = 10
)
purrr::insistently(
  sportsdataversedata::sportsdataverse_save,
  rate = retry_rate,
  quiet = FALSE
)(
  data_frame = final_sched %>%
    dplyr::filter(.data$PBP == TRUE),
  file_name = glue::glue("mbb_games_in_data_repo"),
  sportsdataverse_type = "schedule data",
  release_tag = "espn_mens_college_basketball_schedules",
  pkg_function = "hoopR::load_mbb_schedule()",
  file_types = c("rds", "csv", "parquet"),
  .token = Sys.getenv("GITHUB_PAT")
)


cli::cli_progress_message("")

rm(sched_g)
rm(final_sched)
rm(sched_list)
rm(years_vec)
rm(all_games)
gcol <- gc()
