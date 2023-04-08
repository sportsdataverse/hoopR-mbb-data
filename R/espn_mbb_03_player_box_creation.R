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
  make_option(c("-s", "--start_year"),
              action = "store",
              default = hoopR:::most_recent_mbb_season(),
              type = "integer",
              help = "Start year of the seasons to process"),
  make_option(c("-e", "--end_year"),
              action = "store",
              default = hoopR:::most_recent_mbb_season(),
              type = "integer",
              help = "End year of the seasons to process")
)
opt <- parse_args(OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- opt$s:opt$e

# --- compile into player_box_{year}.parquet ---------

mbb_player_box_games <- function(y) {

  espn_df <- data.frame()
  sched <- hoopR:::rds_from_url(paste0("https://raw.githubusercontent.com/sportsdataverse/hoopR-mbb-raw/main/mbb/schedules/rds/mbb_schedule_", y, ".rds"))

  season_player_box_list <- sched %>%
    dplyr::filter(.data$game_json == TRUE) %>%
    dplyr::pull("game_id")


  if (length(season_player_box_list) > 0) {

    cli::cli_progress_step(msg = "Compiling {y} ESPN MBB Player Boxscores ({length(season_player_box_list)} games)",
                          msg_done = "Compiled {y} ESPN MBB Player Boxscores!")

    future::plan("multisession")
    espn_df <- furrr::future_map_dfr(season_player_box_list, function(x) {
      tryCatch(
        expr = {
          resp <- glue::glue("https://raw.githubusercontent.com/sportsdataverse/hoopR-mbb-raw/main/mbb/json/final/{x}.json")
          player_box_score <- hoopR:::helper_espn_mbb_player_box(resp)
          return(player_box_score)
        },
        error = function(e) {
          message(glue::glue("{Sys.time()}: Player box score data for {x} issue!"))
        }
      )
    }, .options = furrr::furrr_options(seed = TRUE))

    cli::cli_progress_step(msg = "Updating {y} ESPN MBB Player Boxscores GitHub Release",
                           msg_done = "Updated {y} ESPN MBB Player Boxscores GitHub Release!")

  }
  if (nrow(espn_df) > 1) {

    espn_df <- espn_df %>%
      dplyr::arrange(dplyr::desc(.data$game_date)) %>%
      hoopR:::make_hoopR_data("ESPN MBB Player Boxscores from hoopR data repository", Sys.time())

    ifelse(!dir.exists(file.path("mbb/player_box")), dir.create(file.path("mbb/player_box")), FALSE)

    # ifelse(!dir.exists(file.path("mbb/player_box/csv")), dir.create(file.path("mbb/player_box/csv")), FALSE)
    # data.table::fwrite(espn_df, file = paste0("mbb/player_box/csv/player_box_", y, ".csv.gz"))

    # ifelse(!dir.exists(file.path("mbb/player_box/qs")), dir.create(file.path("mbb/player_box/qs")), FALSE)
    # qs::qsave(espn_df, glue::glue("mbb/player_box/qs/player_box_{y}.qs"))

    ifelse(!dir.exists(file.path("mbb/player_box/rds")), dir.create(file.path("mbb/player_box/rds")), FALSE)
    saveRDS(espn_df, glue::glue("mbb/player_box/rds/player_box_{y}.rds"))

    ifelse(!dir.exists(file.path("mbb/player_box/parquet")), dir.create(file.path("mbb/player_box/parquet")), FALSE)
    arrow::write_parquet(espn_df, glue::glue("mbb/player_box/parquet/player_box_{y}.parquet"))

    sportsdataversedata::sportsdataverse_save(
      data_frame = espn_df,
      file_name =  glue::glue("player_box_{y}"),
      sportsdataverse_type = "player boxscores data",
      release_tag = "espn_mens_college_basketball_player_boxscores",
      pkg_function = "hoopR::load_mbb_player_box()",
      file_types = c("rds", "csv", "parquet"),
      .token = Sys.getenv("GITHUB_PAT")
    )
  }

  sched <- sched %>%
    dplyr::mutate(dplyr::across(dplyr::any_of(c(
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
    )), ~as.integer(.x))) %>%
    dplyr::mutate(
      status_display_clock = as.character(.data$status_display_clock),
      game_date_time = lubridate::ymd_hm(substr(.data$date, 1, nchar(.data$date) - 1)) %>%
        lubridate::with_tz(tzone = "America/New_York"),
      game_date = as.Date(substr(.data$game_date_time, 1, 10)))

  if (nrow(espn_df) > 0) {

    sched <- sched %>%
      dplyr::mutate(
        player_box = ifelse(.data$game_id %in% unique(espn_df$game_id), TRUE, FALSE))

  } else {

    cli::cli_alert_info("{length(season_player_box_list)} ESPN MBB Player Boxscores to be compiled for {y}, skipping Player Boxscores compilation")
    sched$player_box <- FALSE

  }

  final_sched <- sched %>%
    dplyr::distinct() %>%
    dplyr::arrange(dplyr::desc(.data$date))

  final_sched <- final_sched %>%
    hoopR:::make_hoopR_data("ESPN MBB Schedule from hoopR data repository", Sys.time())

  sportsdataversedata::sportsdataverse_save(
    data_frame = final_sched,
    file_name =  glue::glue("mbb_schedule_{y}"),
    sportsdataverse_type = "schedule data",
    release_tag = "espn_mens_college_basketball_schedules",
    pkg_function = "hoopR::load_mbb_schedule()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  # data.table::fwrite(final_sched, paste0("mbb/schedules/csv/mbb_schedule_", y, ".csv"))
  # qs::qsave(final_sched, glue::glue("mbb/schedules/qs/mbb_schedule_{y}.qs"))
  saveRDS(final_sched, glue::glue("mbb/schedules/rds/mbb_schedule_{y}.rds"))
  arrow::write_parquet(final_sched, glue::glue("mbb/schedules/parquet/mbb_schedule_{y}.parquet"))
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

cli::cli_progress_step(msg = "Compiling ESPN MBB master schedule",
                       msg_done = "ESPN MBB master schedule compiled and written to disk")

sched_list <- list.files(path = glue::glue("mbb/schedules/parquet/"))
sched_g <-  purrr::map_dfr(sched_list, function(x) {
  sched <- arrow::read_parquet(paste0("mbb/schedules/parquet/", x)) %>%
    dplyr::mutate(dplyr::across(dplyr::any_of(c(
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
    )), ~as.integer(.x))) %>%
    dplyr::mutate(
      status_display_clock = as.character(.data$status_display_clock),
      game_date_time = lubridate::ymd_hm(substr(.data$date, 1, nchar(.data$date) - 1)) %>%
        lubridate::with_tz(tzone = "America/New_York"),
      game_date = as.Date(substr(.data$game_date_time, 1, 10)))
  return(sched)
})

sched_g <- sched_g %>%
  hoopR:::make_hoopR_data("ESPN MBB Schedule from hoopR data repository", Sys.time())

# data.table::fwrite(sched_g %>% dplyr::arrange(desc(.data$date)), "mbb/mbb_schedule_master.csv")
data.table::fwrite(sched_g %>%
                     dplyr::filter(.data$PBP == TRUE) %>%
                     dplyr::arrange(dplyr::desc(.data$date)), "mbb/mbb_games_in_data_repo.csv")
arrow::write_parquet(sched_g %>%
                       dplyr::arrange(dplyr::desc(.data$date)), glue::glue("mbb/mbb_schedule_master.parquet"))
arrow::write_parquet(sched_g %>%
                       dplyr::filter(.data$PBP == TRUE) %>%
                       dplyr::arrange(dplyr::desc(.data$date)), "mbb/mbb_games_in_data_repo.parquet")

cli::cli_progress_message("")

rm(sched_g)
rm(sched_list)
rm(years_vec)
rm(all_games)
gcol <- gc()