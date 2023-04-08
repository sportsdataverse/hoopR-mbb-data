# hoopR-mbb-data
hoopR Men's College Basketball Data 2003 - Present


```mermaid
  graph LR;
    A[hoopR-mbb-raw]-->B[hoopR-mbb-data];
    B[hoopR-mbb-data]-->C1[espn_mens_college_basketball_pbp];
    B[hoopR-mbb-data]-->C2[espn_mens_college_basketball_team_boxscores];
    B[hoopR-mbb-data]-->C3[espn_mens_college_basketball_player_boxscores];

```

```mermaid
flowchart TB;
    subgraph A[hoopR-mbb-raw];
        direction TB;
        A1[python/scrape_mbb_schedules.py]-->A2[python/scrape_mbb_json.py];
    end;

    subgraph B[hoopR-mbb-data];
        direction TB;
        B1[R/espn_mbb_01_pbp_creation.R]-->B2[R/espn_mbb_02_team_box_creation.R];
        B2[R/espn_mbb_02_team_box_creation.R]-->B3[R/espn_mbb_03_player_box_creation.R];
    end;

    subgraph C[sportsdataverse Releases];
        direction TB;
        C1[espn_mens_college_basketball_pbp];
        C2[espn_mens_college_basketball_team_boxscores];
        C3[espn__mens_college_basketball_player_boxscores];
    end;

    A-->B;
    B-->C1;
    B-->C2;
    B-->C3;

```

[hoopR-nba-raw data repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-nba-raw)

[hoopR-nba-data repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-nba-data)

[hoopR-nba-stats-data Repo (source: NBA Stats)](https://github.com/sportsdataverse/hoopR-nba-stats-data)

[hoopR-mbb-raw data repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-mbb-raw)

[hoopR-mbb-data repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-mbb-data)

[hoopR-kp-data Repo (source: KenPom)](https://github.com/sportsdataverse/hoopR-kp-data)