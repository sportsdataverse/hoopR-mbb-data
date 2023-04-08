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

# --- compile into team_box_{year}.parquet ---------

mbb_team_box_games <- function(y) {

  espn_df <- data.frame()
  sched <- readRDS(paste0("mbb/schedules/rds/mbb_schedule_", y, ".rds"))

  season_team_box_list <- sched %>%
    dplyr::filter(.data$game_json == TRUE) %>%
    dplyr::pull("game_id")

  if (length(season_team_box_list) > 0) {

    cli::cli_progress_step(msg = "Compiling {y} ESPN MBB Team Boxscores ({length(season_team_box_list)} games)",
                           msg_done = "Compiled {y} ESPN MBB Team Boxscores!")

    future::plan("multisession")
    espn_df <- furrr::future_map_dfr(season_team_box_list, function(x) {
      tryCatch(
        expr = {
          resp <- glue::glue("https://raw.githubusercontent.com/sportsdataverse/hoopR-mbb-raw/main/mbb/json/final/{x}.json")
          team_box_score <- hoopR:::helper_espn_mbb_team_box(resp)
          return(team_box_score)
        },
        error = function(e) {
          message(glue::glue("{Sys.time()}: game_id {x} team box issue with error {e}"))
        }
      )
    }, .options = furrr::furrr_options(seed = TRUE))

    if (nrow(espn_df) > 0 && !("largest_lead" %in% colnames(espn_df))) {
      espn_df$largest_lead <- NA_character_
      espn_df <- espn_df %>%
        dplyr::relocate("largest_lead", .after = last_col())
    }

    cli::cli_progress_step(msg = "Updating {y} ESPN MBB Team Boxscores GitHub Release",
                          msg_done = "Updated {y} ESPN MBB Team Boxscores GitHub Release!")

  }
  if (nrow(espn_df) > 0) {

    espn_df <- espn_df %>%
      dplyr::arrange(dplyr::desc(.data$game_date)) %>%
      hoopR:::make_hoopR_data("ESPN MBB Team Boxscores from hoopR data repository", Sys.time())

    ifelse(!dir.exists(file.path("mbb/team_box")), dir.create(file.path("mbb/team_box")), FALSE)
    # ifelse(!dir.exists(file.path("mbb/team_box/csv")), dir.create(file.path("mbb/team_box/csv")), FALSE)
    # data.table::fwrite(espn_df, file = paste0("mbb/team_box/csv/team_box_", y, ".csv.gz"))

    ifelse(!dir.exists(file.path("mbb/team_box/rds")), dir.create(file.path("mbb/team_box/rds")), FALSE)
    saveRDS(espn_df, glue::glue("mbb/team_box/rds/team_box_{y}.rds"))

    ifelse(!dir.exists(file.path("mbb/team_box/parquet")), dir.create(file.path("mbb/team_box/parquet")), FALSE)
    arrow::write_parquet(espn_df, glue::glue("mbb/team_box/parquet/team_box_{y}.parquet"))

    sportsdataversedata::sportsdataverse_save(
      data_frame = espn_df,
      file_name =  glue::glue("team_box_{y}"),
      sportsdataverse_type = "team boxscores data",
      release_tag = "espn_mens_college_basketball_team_boxscores",
      pkg_function = "hoopR::load_mbb_team_box()",
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
        team_box = ifelse(.data$game_id %in% unique(espn_df$game_id), TRUE, FALSE))

  } else {

    cli::cli_alert_info("{length(season_team_box_list)} ESPN MBB Team Boxscores to be compiled for {y}, skipping Team Boxscores compilation")
    sched$team_box <- FALSE

  }

  final_sched <- sched %>%
    dplyr::distinct() %>%
    dplyr::arrange(dplyr::desc(.data$date))

  final_sched <- final_sched %>%
    hoopR:::make_hoopR_data("ESPN MBB Schedule from hoopR data repository", Sys.time())

  ifelse(!dir.exists(file.path("mbb/schedules")), dir.create(file.path("mbb/schedules")), FALSE)
  ifelse(!dir.exists(file.path("mbb/schedules/rds")), dir.create(file.path("mbb/schedules/rds")), FALSE)
  ifelse(!dir.exists(file.path("mbb/schedules/parquet")), dir.create(file.path("mbb/schedules/parquet")), FALSE)
  saveRDS(final_sched, glue::glue("mbb/schedules/rds/mbb_schedule_{y}.rds"))
  arrow::write_parquet(final_sched, glue::glue("mbb/schedules/parquet/mbb_schedule_{y}.parquet"))
  rm(sched)
  rm(final_sched)
  rm(espn_df)
  gc()

  return(NULL)
}

tictoc::tic()
all_games <- purrr::map(years_vec, function(y) {
  mbb_team_box_games(y)
  return(NULL)
})
tictoc::toc()


cli::cli_progress_step(msg = "Compiling ESPN MBB master schedule",
                       msg_done = "ESPN MBB master schedule compiled and written to disk")

sched_list <- list.files(path = glue::glue("mbb/schedules/rds/"))
sched_g <-  purrr::map_dfr(sched_list, function(x) {
  sched <- readRDS(paste0("mbb/schedules/rds/", x)) %>%
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

rm(all_games)
rm(sched_g)
rm(sched_list)
rm(years_vec)
gcol <- gc()
