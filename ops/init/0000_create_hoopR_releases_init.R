
#--- ESPN MBB Data -----
piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_mens_college_basketball_schedules",
  name = "espn_mens_college_basketball_schedules",
  body = "NCAA Men's College Basketball Schedules Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_mens_college_basketball_team_boxscores",
  name = "espn_mens_college_basketball_team_boxscores",
  body = "NCAA Men's College Basketball Team Boxscores Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_mens_college_basketball_player_boxscores",
  name = "espn_mens_college_basketball_player_boxscores",
  body = "NCAA Men's College Basketball Player Boxscores Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)


piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_mens_college_basketball_pbp",
  name = "espn_mens_college_basketball_pbp",
  body = "NCAA Men's College Basketball Play-by-Play Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)

#--- ESPN NBA Data -----

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_nba_schedules",
  name = "espn_nba_schedules",
  body = "NBA Schedules Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_nba_team_boxscores",
  name = "espn_nba_team_boxscores",
  body = "NBA Team Boxscores Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_nba_player_boxscores",
  name = "espn_nba_player_boxscores",
  body = "NBA Player Boxscores Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)


piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_nba_pbp",
  name = "espn_nba_pbp",
  body = "NBA Play-by-Play Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)



#--- NBA Stats Data -----

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "nba_stats_schedules",
  name = "nba_stats_schedules",
  body = "NBA Schedules Data (from stats.nba.com)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "nba_stats_team_boxscores",
  name = "nba_stats_team_boxscores",
  body = "NBA Team Boxscores Data (from stats.nba.com)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "nba_stats_player_boxscores",
  name = "nba_stats_player_boxscores",
  body = "NBA Player Boxscores Data (from stats.nba.com)",
  .token = Sys.getenv("GITHUB_PAT")
)


piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "nba_stats_pbp",
  name = "nba_stats_pbp",
  body = "NBA Play-by-Play Data (from stats.nba.com)",
  .token = Sys.getenv("GITHUB_PAT")
)

#--- ESPN MBB Data (added 2026-05: standings/season-stats/rosters/per-game) -----

local({
  mbb_new <- list(
    espn_mens_college_basketball_shots                = "MBB Shots Data (from ESPN)",
    espn_mens_college_basketball_rosters              = "MBB Team Rosters Data (from ESPN)",
    espn_mens_college_basketball_player_season_stats  = "MBB Player Season Stats Data (from ESPN)",
    espn_mens_college_basketball_team_season_stats    = "MBB Team Season Stats Data (from ESPN)",
    espn_mens_college_basketball_standings            = "MBB Standings Data (from ESPN)",
    espn_mens_college_basketball_game_rosters         = "MBB Per-Game Rosters Data (from ESPN)",
    espn_mens_college_basketball_officials            = "MBB Per-Game Officials Data (from ESPN)"
  )
  for (tag in names(mbb_new)) {
    tryCatch(
      piggyback::pb_release_create(
        repo = "sportsdataverse/sportsdataverse-data",
        tag = tag,
        name = tag,
        body = mbb_new[[tag]],
        .token = Sys.getenv("GITHUB_PAT")
      ),
      error = function(e) message(sprintf("%s: %s", tag, conditionMessage(e)))
    )
  }
})
