# MBB opponent-adjusted team ratings


Per-season opponent-adjusted team ratings publish on the `mbb_ratings`
release tag: offensive and defensive efficiency (`adj_o` / `adj_d`,
points per 100 possessions) adjusted for opponent quality, plus adjusted
tempo, a net rating (`adj_em`), and its z-score. The engine is the
sdv-py MBB prediction stack’s iterative opponent adjustment — the
em-scale fixed-point solver — so a team’s number reflects who it played,
not just what it scored. Ratings are recomputed from scratch (not
incrementally updated) on every run, so late corrections to the
underlying published pbp/box data propagate automatically.

The model deliberately has no hidden machinery to explain: it is a
fixed-point solve over the season-to-date game matrix. Its “features”
are the game results themselves, its attribution is the O/D
decomposition it publishes, and its verification is external — the
engine’s oracle gates in sdv-py hold the season ordering against
KenPom/Torvik-class references where they are trained. What this
document adds is the render-time view of the published assets a consumer
actually downloads: internal-consistency checks, the structure of the
rating surface, and identified team-level results.

## Training data

<div id="ukxekrpndi" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ukxekrpndi table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ukxekrpndi thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ukxekrpndi p { margin: 0; padding: 0; }
 #ukxekrpndi .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ukxekrpndi .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ukxekrpndi .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ukxekrpndi .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ukxekrpndi .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ukxekrpndi .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ukxekrpndi .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ukxekrpndi .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ukxekrpndi .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ukxekrpndi .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ukxekrpndi .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ukxekrpndi .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ukxekrpndi .gt_spanner_row { border-bottom-style: hidden; }
 #ukxekrpndi .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ukxekrpndi .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ukxekrpndi .gt_from_md> :first-child { margin-top: 0; }
 #ukxekrpndi .gt_from_md> :last-child { margin-bottom: 0; }
 #ukxekrpndi .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ukxekrpndi .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ukxekrpndi .gt_indent_1 { text-indent: 5px; }
 #ukxekrpndi .gt_indent_2 { text-indent: calc(5px * 2); }
 #ukxekrpndi .gt_indent_3 { text-indent: calc(5px * 3); }
 #ukxekrpndi .gt_indent_4 { text-indent: calc(5px * 4); }
 #ukxekrpndi .gt_indent_5 { text-indent: calc(5px * 5); }
 #ukxekrpndi .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ukxekrpndi .gt_row_group_first td { border-top-width: 2px; }
 #ukxekrpndi .gt_row_group_first th { border-top-width: 2px; }
 #ukxekrpndi .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ukxekrpndi .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ukxekrpndi .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ukxekrpndi .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ukxekrpndi .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ukxekrpndi .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ukxekrpndi .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ukxekrpndi .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ukxekrpndi .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ukxekrpndi .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ukxekrpndi .gt_left { text-align: left; }
 #ukxekrpndi .gt_center { text-align: center; }
 #ukxekrpndi .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ukxekrpndi .gt_font_normal { font-weight: normal; }
 #ukxekrpndi .gt_font_bold { font-weight: bold; }
 #ukxekrpndi .gt_font_italic { font-style: italic; }
 #ukxekrpndi .gt_super { font-size: 65%; }
 #ukxekrpndi .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ukxekrpndi .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ukxekrpndi .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ukxekrpndi .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ukxekrpndi .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ukxekrpndi .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Published mbb_ratings assets, by season |  |  |  |
|----|----|----|----|
| computed at render time from the release; adj_em is mean-zero by construction |  |  |  |
| season | teams | team_games | mean_adj_em |
| 2020 | 658 | 11,514 | −21.271 |
| 2021 | 493 | 8,566 | −11.456 |
| 2022 | 679 | 11,930 | −22.178 |
| 2023 | 706 | 12,440 | −23.360 |
| 2024 | 717 | 12,480 | −23.974 |
| 2025 | 700 | 12,572 | −24.646 |
| 2026 | 727 | 12,598 | −25.751 |

&#10;</div>

Inputs are the published season pbp/box assets of this repository — the
ratings sit downstream of the same daily pipeline that publishes the
data they are computed from, which is what keeps them reproducible.

## Exploratory data analysis

<img src="ratings_files/figure-commonmark/cell-4-output-1.png"
width="420" height="300"
alt="The rating surface: adjusted offense vs adjusted defense (defense lower = better), latest season." />

<img src="ratings_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300"
alt="Adjustment at work: adjusted net vs raw net. Off-diagonal teams are schedule effects." />

<div id="oawuehjjey" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#oawuehjjey table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#oawuehjjey thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#oawuehjjey p { margin: 0; padding: 0; }
 #oawuehjjey .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #oawuehjjey .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #oawuehjjey .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #oawuehjjey .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #oawuehjjey .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #oawuehjjey .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #oawuehjjey .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #oawuehjjey .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #oawuehjjey .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #oawuehjjey .gt_column_spanner_outer:first-child { padding-left: 0; }
 #oawuehjjey .gt_column_spanner_outer:last-child { padding-right: 0; }
 #oawuehjjey .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #oawuehjjey .gt_spanner_row { border-bottom-style: hidden; }
 #oawuehjjey .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #oawuehjjey .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #oawuehjjey .gt_from_md> :first-child { margin-top: 0; }
 #oawuehjjey .gt_from_md> :last-child { margin-bottom: 0; }
 #oawuehjjey .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #oawuehjjey .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #oawuehjjey .gt_indent_1 { text-indent: 5px; }
 #oawuehjjey .gt_indent_2 { text-indent: calc(5px * 2); }
 #oawuehjjey .gt_indent_3 { text-indent: calc(5px * 3); }
 #oawuehjjey .gt_indent_4 { text-indent: calc(5px * 4); }
 #oawuehjjey .gt_indent_5 { text-indent: calc(5px * 5); }
 #oawuehjjey .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #oawuehjjey .gt_row_group_first td { border-top-width: 2px; }
 #oawuehjjey .gt_row_group_first th { border-top-width: 2px; }
 #oawuehjjey .gt_striped { color: #333333; background-color: #F4F4F4; }
 #oawuehjjey .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #oawuehjjey .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #oawuehjjey .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #oawuehjjey .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #oawuehjjey .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #oawuehjjey .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #oawuehjjey .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #oawuehjjey .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #oawuehjjey .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #oawuehjjey .gt_left { text-align: left; }
 #oawuehjjey .gt_center { text-align: center; }
 #oawuehjjey .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #oawuehjjey .gt_font_normal { font-weight: normal; }
 #oawuehjjey .gt_font_bold { font-weight: bold; }
 #oawuehjjey .gt_font_italic { font-style: italic; }
 #oawuehjjey .gt_super { font-size: 65%; }
 #oawuehjjey .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #oawuehjjey .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #oawuehjjey .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #oawuehjjey .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #oawuehjjey .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #oawuehjjey .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Internal consistency — 2026           |          |
|---------------------------------------|----------|
| check                                 | value    |
| mean adj_em (should be ~0)            | −25.7506 |
| corr(adj_em, raw margin)              | 0.9544   |
| corr(adj_em, adj_em_z) (should be ~1) | 1.0000   |

&#10;</div>

The vertical spread between raw and adjusted margin is the point of the
model: mid-major teams with gaudy raw margins move down,
power-conference teams with brutal schedules move up, and the
correlation between the two — strong but visibly below 1 — is the honest
measure of how much schedule matters in a 360+ team league where most
games are intra-tier.

## Evaluation

The engine’s publish gates live in sdv-py where it is trained (external
ordering checks against reference systems; the NCAA RAPM repos hold the
same family of engines to Torvik at Spearman ≥ 0.93). At the asset
level, the render-time check available without an external feed is
**predictive consistency**: within a season, adj_em should order
head-to-head margins better than raw margin does, and across seasons a
program’s rating should be sticky. The cross-season stability computed
from the published assets:

<div id="fgvatcbsdq" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#fgvatcbsdq table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#fgvatcbsdq thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#fgvatcbsdq p { margin: 0; padding: 0; }
 #fgvatcbsdq .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #fgvatcbsdq .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #fgvatcbsdq .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #fgvatcbsdq .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #fgvatcbsdq .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #fgvatcbsdq .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #fgvatcbsdq .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #fgvatcbsdq .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #fgvatcbsdq .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #fgvatcbsdq .gt_column_spanner_outer:first-child { padding-left: 0; }
 #fgvatcbsdq .gt_column_spanner_outer:last-child { padding-right: 0; }
 #fgvatcbsdq .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #fgvatcbsdq .gt_spanner_row { border-bottom-style: hidden; }
 #fgvatcbsdq .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #fgvatcbsdq .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #fgvatcbsdq .gt_from_md> :first-child { margin-top: 0; }
 #fgvatcbsdq .gt_from_md> :last-child { margin-bottom: 0; }
 #fgvatcbsdq .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #fgvatcbsdq .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #fgvatcbsdq .gt_indent_1 { text-indent: 5px; }
 #fgvatcbsdq .gt_indent_2 { text-indent: calc(5px * 2); }
 #fgvatcbsdq .gt_indent_3 { text-indent: calc(5px * 3); }
 #fgvatcbsdq .gt_indent_4 { text-indent: calc(5px * 4); }
 #fgvatcbsdq .gt_indent_5 { text-indent: calc(5px * 5); }
 #fgvatcbsdq .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #fgvatcbsdq .gt_row_group_first td { border-top-width: 2px; }
 #fgvatcbsdq .gt_row_group_first th { border-top-width: 2px; }
 #fgvatcbsdq .gt_striped { color: #333333; background-color: #F4F4F4; }
 #fgvatcbsdq .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #fgvatcbsdq .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #fgvatcbsdq .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #fgvatcbsdq .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #fgvatcbsdq .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #fgvatcbsdq .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #fgvatcbsdq .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #fgvatcbsdq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #fgvatcbsdq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #fgvatcbsdq .gt_left { text-align: left; }
 #fgvatcbsdq .gt_center { text-align: center; }
 #fgvatcbsdq .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #fgvatcbsdq .gt_font_normal { font-weight: normal; }
 #fgvatcbsdq .gt_font_bold { font-weight: bold; }
 #fgvatcbsdq .gt_font_italic { font-style: italic; }
 #fgvatcbsdq .gt_super { font-size: 65%; }
 #fgvatcbsdq .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #fgvatcbsdq .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #fgvatcbsdq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #fgvatcbsdq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #fgvatcbsdq .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #fgvatcbsdq .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Program stickiness — adj_em season S vs S+1, same team |  |  |
|----|----|----|
| published-asset check; programs are persistent, rosters are not — the gap is roster turnover |  |  |
| season | yoy_pearson | teams |
| 2020 | 0.896 | 410 |
| 2021 | 0.883 | 423 |
| 2022 | 0.884 | 556 |
| 2023 | 0.884 | 578 |
| 2024 | 0.890 | 572 |
| 2025 | 0.897 | 577 |

&#10;</div>

## Results

<div id="anjhpuapso" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#anjhpuapso table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#anjhpuapso thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#anjhpuapso p { margin: 0; padding: 0; }
 #anjhpuapso .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #anjhpuapso .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #anjhpuapso .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #anjhpuapso .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #anjhpuapso .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #anjhpuapso .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #anjhpuapso .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #anjhpuapso .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #anjhpuapso .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #anjhpuapso .gt_column_spanner_outer:first-child { padding-left: 0; }
 #anjhpuapso .gt_column_spanner_outer:last-child { padding-right: 0; }
 #anjhpuapso .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #anjhpuapso .gt_spanner_row { border-bottom-style: hidden; }
 #anjhpuapso .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #anjhpuapso .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #anjhpuapso .gt_from_md> :first-child { margin-top: 0; }
 #anjhpuapso .gt_from_md> :last-child { margin-bottom: 0; }
 #anjhpuapso .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #anjhpuapso .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #anjhpuapso .gt_indent_1 { text-indent: 5px; }
 #anjhpuapso .gt_indent_2 { text-indent: calc(5px * 2); }
 #anjhpuapso .gt_indent_3 { text-indent: calc(5px * 3); }
 #anjhpuapso .gt_indent_4 { text-indent: calc(5px * 4); }
 #anjhpuapso .gt_indent_5 { text-indent: calc(5px * 5); }
 #anjhpuapso .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #anjhpuapso .gt_row_group_first td { border-top-width: 2px; }
 #anjhpuapso .gt_row_group_first th { border-top-width: 2px; }
 #anjhpuapso .gt_striped { color: #333333; background-color: #F4F4F4; }
 #anjhpuapso .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #anjhpuapso .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #anjhpuapso .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #anjhpuapso .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #anjhpuapso .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #anjhpuapso .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #anjhpuapso .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #anjhpuapso .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #anjhpuapso .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #anjhpuapso .gt_left { text-align: left; }
 #anjhpuapso .gt_center { text-align: center; }
 #anjhpuapso .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #anjhpuapso .gt_font_normal { font-weight: normal; }
 #anjhpuapso .gt_font_bold { font-weight: bold; }
 #anjhpuapso .gt_font_italic { font-style: italic; }
 #anjhpuapso .gt_super { font-size: 65%; }
 #anjhpuapso .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #anjhpuapso .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #anjhpuapso .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #anjhpuapso .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #anjhpuapso .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #anjhpuapso .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Top 25 — 2026 adjusted ratings |  |  |  |  |  |  |  |
|----|----|----|----|----|----|----|----|
|  | Team | Rk | AdjO | AdjD | AdjEM | AdjT | G |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/130.png"
height="36" /> | Michigan Wolverines | 1 | 132.9 | 85.5 | 47.4 | 71.7 | 40 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/150.png"
height="36" /> | Duke Blue Devils | 2 | 131.9 | 86.5 | 45.4 | 66.3 | 38 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/12.png"
height="36" /> | Arizona Wildcats | 3 | 130.2 | 86.8 | 43.4 | 70.8 | 39 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/356.png"
height="36" /> | Illinois Fighting Illini | 4 | 134.5 | 93.7 | 40.8 | 67.0 | 37 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/57.png"
height="36" /> | Florida Gators | 5 | 129.5 | 89.2 | 40.4 | 70.6 | 35 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/248.png"
height="36" /> | Houston Cougars | 6 | 127.4 | 87.1 | 40.2 | 63.9 | 37 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/66.png"
height="36" /> | Iowa State Cyclones | 7 | 128.1 | 89.1 | 39.0 | 67.9 | 37 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/2509.png"
height="36" /> | Purdue Boilermakers | 8 | 134.8 | 96.9 | 37.8 | 65.3 | 39 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/41.png"
height="36" /> | UConn Huskies | 9 | 125.9 | 90.0 | 35.8 | 65.5 | 40 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/2250.png"
height="36" /> | Gonzaga Bulldogs | 10 | 124.8 | 89.1 | 35.6 | 69.7 | 35 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/127.png"
height="36" /> | Michigan State Spartans | 11 | 125.8 | 90.6 | 35.2 | 67.2 | 35 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/333.png"
height="36" /> | Alabama Crimson Tide | 12 | 132.4 | 99.0 | 33.5 | 73.7 | 35 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/2633.png"
height="36" /> | Tennessee Volunteers | 13 | 124.6 | 91.6 | 33.1 | 66.5 | 37 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/238.png"
height="36" /> | Vanderbilt Commodores | 14 | 129.9 | 97.0 | 33.0 | 69.6 | 36 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/2599.png"
height="36" /> | St. John's Red Storm | 15 | 124.1 | 91.3 | 32.7 | 70.2 | 37 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/97.png"
height="36" /> | Louisville Cardinals | 16 | 127.3 | 94.9 | 32.4 | 70.3 | 35 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/158.png"
height="36" /> | Nebraska Cornhuskers | 17 | 121.9 | 89.6 | 32.3 | 67.6 | 35 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/258.png"
height="36" /> | Virginia Cavaliers | 18 | 126.3 | 94.7 | 31.6 | 67.3 | 36 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/8.png"
height="36" /> | Arkansas Razorbacks | 19 | 131.0 | 99.4 | 31.6 | 72.7 | 37 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/2641.png"
height="36" /> | Texas Tech Red Raiders | 20 | 127.7 | 96.5 | 31.2 | 67.0 | 34 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/2305.png"
height="36" /> | Kansas Jayhawks | 21 | 120.8 | 90.3 | 30.5 | 68.8 | 35 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/252.png"
height="36" /> | BYU Cougars | 22 | 128.5 | 98.7 | 29.9 | 70.7 | 35 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/2294.png"
height="36" /> | Iowa Hawkeyes | 23 | 127.3 | 97.6 | 29.7 | 63.3 | 37 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/275.png"
height="36" /> | Wisconsin Badgers | 24 | 127.9 | 99.4 | 28.5 | 70.2 | 35 |
| <img src="https://a.espncdn.com/i/teamlogos/ncaa/500/96.png"
height="36" /> | Kentucky Wildcats | 25 | 124.3 | 95.9 | 28.4 | 68.8 | 36 |

&#10;</div>

## Provenance & reproducibility

- **Computed from:** this repository’s published season pbp/box assets,
  seasons listed in the corpus table; recomputed in full on every run.
- **Engine:** the sdv-py MBB prediction stack’s iterative opponent
  adjustment (em-scale fixed point); engine training + oracle gates live
  in sdv-py.
- **Pipeline:** `scripts/mbb_models.sh 01` → stage
  `python/mbb_model_01_ratings.py` (wired via `mbb_models_cron.yml`);
  each publish writes a card sidecar
  ([`mbb_models_eval_card.json`](mbb_models_eval_card.json)). Single
  home: `models/manifest.yaml`.
- **Rebuild this document:** `scripts/render_model_docs.sh` (Quarto →
  GFM; `uv sync --group docs`). Requires network for the release
  download and the logo CDN.

## Avenues for improvement & open issues

- **Preseason priors** — blend the recruiting/returning-production prior
  into early-season ratings instead of starting from a flat matrix.
- **Home/travel modeling** — altitude and travel distance are unmodeled.
- **Known issue:** Spearman-style external checks are scale-blind; the
  level bands that catch scale bugs live in the sdv-py gates, not here.
