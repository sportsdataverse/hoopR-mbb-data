# Rebuild the frozen R pbp oracle from the committed raw fixtures.
#
# Mirrors the per-season compile in R/espn_mbb_01_pbp_creation.R (helper per
# game -> bind_rows -> coordinate fill -> arrange(desc(game_date)) ->
# make_hoopR_data -> arrow::write_parquet), minus the HTTP reads and the
# release upload. Unlike the production loop, a game that errors or warns
# FAILS the capture instead of being silently dropped.
#
# Run from the repo root, against hoopR installed from a pinned checkout:
#   R_LIBS=<scratch lib with hoopR> Rscript tests/fixtures/pbp_r_oracle/build_r_oracle.R
# then update README.md (hoopR commit, R version, capture date).

suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
})
options(scipen = 999)

dir <- file.path("tests", "fixtures", "pbp_r_oracle")
raw <- sort(list.files(file.path(dir, "raw"), pattern = "\\.json\\.gz$", full.names = TRUE))
stopifnot(length(raw) > 0)

espn_df <- purrr::map_dfr(raw, function(f) {
  tmp <- tempfile(fileext = ".json")
  writeLines(readLines(gzfile(f), warn = FALSE, encoding = "UTF-8"), tmp, useBytes = TRUE)
  out <- hoopR:::helper_espn_mbb_pbp(tmp)
  if (is.null(out) || nrow(out) == 0) stop("no plays parsed from ", f)
  out
})

if (!("coordinate_x" %in% colnames(espn_df))) {
  espn_df <- espn_df %>%
    dplyr::mutate(
      coordinate_x = NA_real_,
      coordinate_y = NA_real_,
      coordinate_x_raw = NA_real_,
      coordinate_y_raw = NA_real_
    )
}

espn_df <- espn_df %>%
  dplyr::arrange(dplyr::desc(.data$game_date)) %>%
  hoopR:::make_hoopR_data("ESPN MBB Play-by-Play from hoopR data repository", Sys.time())

arrow::write_parquet(espn_df, file.path(dir, "play_by_play_r_oracle.parquet"))
cat(
  "hoopR", as.character(packageVersion("hoopR")), "|", R.version.string, "|",
  nrow(espn_df), "rows x", ncol(espn_df), "cols |",
  length(unique(espn_df$game_id)), "games\n"
)
