# Bart Torvik team-list snapshots

`{season}_team_results.csv` is Torvik's team ratings file for that season,
committed as served with CRLF normalized to LF (`manifest.json` records each file's URL,
fetch time in UTC, sha256 and team count). The team crosswalk build reads the
`team` / `conf` columns from here (`mbb_data_build.torvik_snapshot.bart_teams`)
instead of calling barttorvik.com, whose CloudFront answers 403 to every
datacenter IP: GitHub runners and the droplet.

Fetched from a residential IP. Refresh the same way, at least once per season
before it starts (a missing season fails the crosswalk build with this command):

```sh
cd python && uv run python -m mbb_data_build.torvik_snapshot -s 2027 -e 2027
```
