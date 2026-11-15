# Bart Torvik snapshots

The builds read these files instead of calling barttorvik.com, whose CloudFront answers 403 to every
datacenter IP (GitHub runners and the droplet). `manifest.json` records each file's URL, its fetch time in
UTC, its sha256, and its team or game count.

- `{season}_team_results.csv` is Torvik's team ratings file, committed as served with CRLF normalized to LF.
  The team and schedule crosswalks read its `team` / `conf` columns
  (`mbb_data_build.torvik_snapshot.bart_teams`).
- `{season}_super_sked.json` is Torvik's season game list, committed as compact JSON. It is the schedule
  crosswalk's game spine (`bart_games`). A season without one makes the schedule crosswalk fetch live, which
  works from a residential IP only.
  Torvik's `winner` is frozen at fetch time, so the build fills `bart_winner` for games finished
  since then from the ESPN result (`mbb_data_build.torvik_snapshot.fill_bart_winner`). Torvik's
  own value is never overwritten.

Fetched from a residential IP. Refresh the same way before each season starts. Refresh again once the
conference and NCAA tournament games are scheduled, because the schedule file holds only the games Torvik
listed when it was fetched:

```sh
cd python && uv run python -m mbb_data_build.torvik_snapshot -s 2027 -e 2027
```
