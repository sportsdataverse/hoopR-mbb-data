

team_box_list <- list.files(path = glue::glue('mbb/json/final/'))
team_box_game_ids <- as.integer(gsub('.json', '', team_box_list))

future::plan("multisession")
espn_df <- furrr::future_map_dfr(team_box_game_ids, function(x){
  resp <- glue::glue('mbb/json/final/{x}.json') %>%
    jsonlite::fromJSON()
  jsonlite::write_json(resp, glue::glue('mbb/json/final/{x}.json'), prettify = 0)

  return(NULL)
}, .options = furrr::furrr_options(seed = TRUE))


team_box_list <- list.files(path = glue::glue('mbb/json/raw/'))
team_box_game_ids <- as.integer(gsub('.json', '', team_box_list))

future::plan("multisession")
espn_df <- furrr::future_map_dfr(team_box_game_ids, function(x){
  resp <- glue::glue('mbb/json/raw/{x}.json') %>%
    jsonlite::fromJSON()
  jsonlite::write_json(resp, glue::glue('mbb/json/raw/{x}.json'), prettify = 0)

  return(NULL)
}, .options = furrr::furrr_options(seed = TRUE))
