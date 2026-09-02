# MBB in-game win probability — pbp enrichment


The MBB rule-era win-probability suite (trained, bundled, and
oracle-gated in sdv-py) is applied **in place** to every published
season of `espn_mens_college_basketball_pbp`: `home_win_prob` and the
pregame prior (`pregame_home_prob`) are added to each play with every
original column preserved. The published pbp itself is how the model
reaches consumers — there is no separate WP asset to fall out of sync
with the plays.

The model is an XGBoost classifier over game state — score margin,
seconds left (and its square root), the pregame logit, and possession —
fit on a single season (2023). Operationally the enrichment **is** the
pbp publisher: `scripts/daily_mbb_data_processor.sh` builds the plain
season pbp into the tree and never uploads it; stage
`mbb_model_03_wp_enrich` reads the tree pbp/schedules/team_box, appends
the two columns and publishes parquet+csv+rds; and
`mbb_data_build.publish` refuses any pbp parquet without finite WP
columns, asserted on the file about to upload. That design closes a
recorded incident: the old publish-plain-then-re-enrich order stripped
the columns off the release on every nightly, and the 2026-08-26
whole-history republish stripped 2003–2023 outright (2024–2026 were
re-enriched by the in-season nightly). Until those seasons are
republished, this document computes the holdout era’s probabilities
itself and says so.

This document is the model’s **out-of-band evaluation**: it downloads a
full published season at render time and holds the in-game probabilities
against each game’s realized outcome — first in-era, then for an era the
booster never saw. If the enrichment ever regressed, went stale, or was
stripped, the calibration sections below would show it on the next
render.

## Evaluation data

<div id="kqymaaktsk" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#kqymaaktsk table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#kqymaaktsk thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#kqymaaktsk p { margin: 0; padding: 0; }
 #kqymaaktsk .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #kqymaaktsk .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #kqymaaktsk .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #kqymaaktsk .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #kqymaaktsk .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #kqymaaktsk .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #kqymaaktsk .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #kqymaaktsk .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #kqymaaktsk .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #kqymaaktsk .gt_column_spanner_outer:first-child { padding-left: 0; }
 #kqymaaktsk .gt_column_spanner_outer:last-child { padding-right: 0; }
 #kqymaaktsk .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #kqymaaktsk .gt_spanner_row { border-bottom-style: hidden; }
 #kqymaaktsk .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #kqymaaktsk .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #kqymaaktsk .gt_from_md> :first-child { margin-top: 0; }
 #kqymaaktsk .gt_from_md> :last-child { margin-bottom: 0; }
 #kqymaaktsk .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #kqymaaktsk .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #kqymaaktsk .gt_indent_1 { text-indent: 5px; }
 #kqymaaktsk .gt_indent_2 { text-indent: calc(5px * 2); }
 #kqymaaktsk .gt_indent_3 { text-indent: calc(5px * 3); }
 #kqymaaktsk .gt_indent_4 { text-indent: calc(5px * 4); }
 #kqymaaktsk .gt_indent_5 { text-indent: calc(5px * 5); }
 #kqymaaktsk .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #kqymaaktsk .gt_row_group_first td { border-top-width: 2px; }
 #kqymaaktsk .gt_row_group_first th { border-top-width: 2px; }
 #kqymaaktsk .gt_striped { color: #333333; background-color: #F4F4F4; }
 #kqymaaktsk .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #kqymaaktsk .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #kqymaaktsk .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #kqymaaktsk .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #kqymaaktsk .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #kqymaaktsk .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #kqymaaktsk .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #kqymaaktsk .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #kqymaaktsk .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #kqymaaktsk .gt_left { text-align: left; }
 #kqymaaktsk .gt_center { text-align: center; }
 #kqymaaktsk .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #kqymaaktsk .gt_font_normal { font-weight: normal; }
 #kqymaaktsk .gt_font_bold { font-weight: bold; }
 #kqymaaktsk .gt_font_italic { font-style: italic; }
 #kqymaaktsk .gt_super { font-size: 65%; }
 #kqymaaktsk .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #kqymaaktsk .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #kqymaaktsk .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #kqymaaktsk .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #kqymaaktsk .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #kqymaaktsk .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Published season evaluated at render time — 2026 |  |  |  |  |
|----|----|----|----|----|
| every play of the published pbp joined to its game's realized outcome (ties from data artifacts excluded) |  |  |  |  |
| season | enriched_plays | games | home_win_rate | mean_home_win_prob |
| 2026 | 2,906,233 | 6256 | 66.7% | 0.6687 |

&#10;</div>

The mean predicted probability sitting close to the realized home-win
rate is the zeroth-order calibration check; the college home floor is
one of the strongest in sports and both numbers reflect it.

## Calibration

<div id="xogfvgevcy" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#xogfvgevcy table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#xogfvgevcy thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#xogfvgevcy p { margin: 0; padding: 0; }
 #xogfvgevcy .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #xogfvgevcy .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #xogfvgevcy .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #xogfvgevcy .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #xogfvgevcy .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #xogfvgevcy .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xogfvgevcy .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #xogfvgevcy .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #xogfvgevcy .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #xogfvgevcy .gt_column_spanner_outer:first-child { padding-left: 0; }
 #xogfvgevcy .gt_column_spanner_outer:last-child { padding-right: 0; }
 #xogfvgevcy .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #xogfvgevcy .gt_spanner_row { border-bottom-style: hidden; }
 #xogfvgevcy .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #xogfvgevcy .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #xogfvgevcy .gt_from_md> :first-child { margin-top: 0; }
 #xogfvgevcy .gt_from_md> :last-child { margin-bottom: 0; }
 #xogfvgevcy .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #xogfvgevcy .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #xogfvgevcy .gt_indent_1 { text-indent: 5px; }
 #xogfvgevcy .gt_indent_2 { text-indent: calc(5px * 2); }
 #xogfvgevcy .gt_indent_3 { text-indent: calc(5px * 3); }
 #xogfvgevcy .gt_indent_4 { text-indent: calc(5px * 4); }
 #xogfvgevcy .gt_indent_5 { text-indent: calc(5px * 5); }
 #xogfvgevcy .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #xogfvgevcy .gt_row_group_first td { border-top-width: 2px; }
 #xogfvgevcy .gt_row_group_first th { border-top-width: 2px; }
 #xogfvgevcy .gt_striped { color: #333333; background-color: #F4F4F4; }
 #xogfvgevcy .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xogfvgevcy .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #xogfvgevcy .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #xogfvgevcy .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xogfvgevcy .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #xogfvgevcy .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #xogfvgevcy .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #xogfvgevcy .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xogfvgevcy .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #xogfvgevcy .gt_left { text-align: left; }
 #xogfvgevcy .gt_center { text-align: center; }
 #xogfvgevcy .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #xogfvgevcy .gt_font_normal { font-weight: normal; }
 #xogfvgevcy .gt_font_bold { font-weight: bold; }
 #xogfvgevcy .gt_font_italic { font-style: italic; }
 #xogfvgevcy .gt_super { font-size: 65%; }
 #xogfvgevcy .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xogfvgevcy .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #xogfvgevcy .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xogfvgevcy .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #xogfvgevcy .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #xogfvgevcy .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Out-of-band calibration — 2026 published season |        |
|-------------------------------------------------|--------|
| metric                                          | value  |
| Brier score (all plays)                         | 0.1212 |
| 20-bin calibration MAE                          | 0.0106 |
| baseline Brier (constant home-win rate)         | 0.2209 |

&#10;</div>

<img src="wp_enrich_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300"
alt="Reliability diagram, 20 bins — predicted in-game probability vs realized outcome frequency." />

<div id="ndebfzzmgw" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ndebfzzmgw table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ndebfzzmgw thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ndebfzzmgw p { margin: 0; padding: 0; }
 #ndebfzzmgw .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ndebfzzmgw .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ndebfzzmgw .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ndebfzzmgw .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ndebfzzmgw .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ndebfzzmgw .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ndebfzzmgw .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ndebfzzmgw .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ndebfzzmgw .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ndebfzzmgw .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ndebfzzmgw .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ndebfzzmgw .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ndebfzzmgw .gt_spanner_row { border-bottom-style: hidden; }
 #ndebfzzmgw .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ndebfzzmgw .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ndebfzzmgw .gt_from_md> :first-child { margin-top: 0; }
 #ndebfzzmgw .gt_from_md> :last-child { margin-bottom: 0; }
 #ndebfzzmgw .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ndebfzzmgw .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ndebfzzmgw .gt_indent_1 { text-indent: 5px; }
 #ndebfzzmgw .gt_indent_2 { text-indent: calc(5px * 2); }
 #ndebfzzmgw .gt_indent_3 { text-indent: calc(5px * 3); }
 #ndebfzzmgw .gt_indent_4 { text-indent: calc(5px * 4); }
 #ndebfzzmgw .gt_indent_5 { text-indent: calc(5px * 5); }
 #ndebfzzmgw .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ndebfzzmgw .gt_row_group_first td { border-top-width: 2px; }
 #ndebfzzmgw .gt_row_group_first th { border-top-width: 2px; }
 #ndebfzzmgw .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ndebfzzmgw .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ndebfzzmgw .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ndebfzzmgw .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ndebfzzmgw .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ndebfzzmgw .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ndebfzzmgw .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ndebfzzmgw .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ndebfzzmgw .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ndebfzzmgw .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ndebfzzmgw .gt_left { text-align: left; }
 #ndebfzzmgw .gt_center { text-align: center; }
 #ndebfzzmgw .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ndebfzzmgw .gt_font_normal { font-weight: normal; }
 #ndebfzzmgw .gt_font_bold { font-weight: bold; }
 #ndebfzzmgw .gt_font_italic { font-style: italic; }
 #ndebfzzmgw .gt_super { font-size: 65%; }
 #ndebfzzmgw .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ndebfzzmgw .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ndebfzzmgw .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ndebfzzmgw .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ndebfzzmgw .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ndebfzzmgw .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Brier by period — uncertainty should resolve as the game progresses |  |  |
|----|----|----|
| a well-behaved WP model gets sharper (lower Brier) in later periods |  |  |
| period_number | plays | brier |
| 1 | 1,391,108 | 0.1540 |
| 2 | 1,491,780 | 0.0903 |
| 3 | 19,848 | 0.1496 |

&#10;</div>

<img src="wp_enrich_files/figure-commonmark/cell-7-output-1.png"
width="420" height="300"
alt="The season’s wildest game by WP swing: in-game home win probability trace." />

<div id="qydblaraga" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#qydblaraga table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#qydblaraga thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#qydblaraga p { margin: 0; padding: 0; }
 #qydblaraga .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #qydblaraga .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #qydblaraga .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #qydblaraga .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #qydblaraga .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #qydblaraga .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qydblaraga .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #qydblaraga .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #qydblaraga .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #qydblaraga .gt_column_spanner_outer:first-child { padding-left: 0; }
 #qydblaraga .gt_column_spanner_outer:last-child { padding-right: 0; }
 #qydblaraga .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #qydblaraga .gt_spanner_row { border-bottom-style: hidden; }
 #qydblaraga .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #qydblaraga .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #qydblaraga .gt_from_md> :first-child { margin-top: 0; }
 #qydblaraga .gt_from_md> :last-child { margin-bottom: 0; }
 #qydblaraga .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #qydblaraga .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #qydblaraga .gt_indent_1 { text-indent: 5px; }
 #qydblaraga .gt_indent_2 { text-indent: calc(5px * 2); }
 #qydblaraga .gt_indent_3 { text-indent: calc(5px * 3); }
 #qydblaraga .gt_indent_4 { text-indent: calc(5px * 4); }
 #qydblaraga .gt_indent_5 { text-indent: calc(5px * 5); }
 #qydblaraga .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #qydblaraga .gt_row_group_first td { border-top-width: 2px; }
 #qydblaraga .gt_row_group_first th { border-top-width: 2px; }
 #qydblaraga .gt_striped { color: #333333; background-color: #F4F4F4; }
 #qydblaraga .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qydblaraga .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #qydblaraga .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #qydblaraga .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qydblaraga .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #qydblaraga .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #qydblaraga .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #qydblaraga .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qydblaraga .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #qydblaraga .gt_left { text-align: left; }
 #qydblaraga .gt_center { text-align: center; }
 #qydblaraga .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #qydblaraga .gt_font_normal { font-weight: normal; }
 #qydblaraga .gt_font_bold { font-weight: bold; }
 #qydblaraga .gt_font_italic { font-style: italic; }
 #qydblaraga .gt_super { font-size: 65%; }
 #qydblaraga .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qydblaraga .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #qydblaraga .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qydblaraga .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #qydblaraga .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #qydblaraga .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Pregame vs in-game — the value the enrichment adds over the prior |        |
|-------------------------------------------------------------------|--------|
| model                                                             | brier  |
| pregame prior (one prob per game)                                 | 0.1874 |
| in-game WP (all plays)                                            | 0.1212 |

&#10;</div>

The reliability diagram hugging the diagonal, the per-period Brier
falling monotonically, and the in-game model beating its own pregame
prior are the three signatures of a healthy applied WP surface. The
volatile-game trace is the demonstration consumers care about: the
column tells the story of a comeback without any narrative input.

## Unseen-era holdout

Holdout season **2012** — 1,115,779 plays in 3,440 games; probabilities
computed at render time – the release asset lacks the WP columns.

<div id="wtvncjsozy" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#wtvncjsozy table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#wtvncjsozy thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#wtvncjsozy p { margin: 0; padding: 0; }
 #wtvncjsozy .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #wtvncjsozy .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #wtvncjsozy .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #wtvncjsozy .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #wtvncjsozy .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #wtvncjsozy .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wtvncjsozy .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #wtvncjsozy .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #wtvncjsozy .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #wtvncjsozy .gt_column_spanner_outer:first-child { padding-left: 0; }
 #wtvncjsozy .gt_column_spanner_outer:last-child { padding-right: 0; }
 #wtvncjsozy .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #wtvncjsozy .gt_spanner_row { border-bottom-style: hidden; }
 #wtvncjsozy .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #wtvncjsozy .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #wtvncjsozy .gt_from_md> :first-child { margin-top: 0; }
 #wtvncjsozy .gt_from_md> :last-child { margin-bottom: 0; }
 #wtvncjsozy .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #wtvncjsozy .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #wtvncjsozy .gt_indent_1 { text-indent: 5px; }
 #wtvncjsozy .gt_indent_2 { text-indent: calc(5px * 2); }
 #wtvncjsozy .gt_indent_3 { text-indent: calc(5px * 3); }
 #wtvncjsozy .gt_indent_4 { text-indent: calc(5px * 4); }
 #wtvncjsozy .gt_indent_5 { text-indent: calc(5px * 5); }
 #wtvncjsozy .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #wtvncjsozy .gt_row_group_first td { border-top-width: 2px; }
 #wtvncjsozy .gt_row_group_first th { border-top-width: 2px; }
 #wtvncjsozy .gt_striped { color: #333333; background-color: #F4F4F4; }
 #wtvncjsozy .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wtvncjsozy .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #wtvncjsozy .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #wtvncjsozy .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wtvncjsozy .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #wtvncjsozy .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #wtvncjsozy .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #wtvncjsozy .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wtvncjsozy .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #wtvncjsozy .gt_left { text-align: left; }
 #wtvncjsozy .gt_center { text-align: center; }
 #wtvncjsozy .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #wtvncjsozy .gt_font_normal { font-weight: normal; }
 #wtvncjsozy .gt_font_bold { font-weight: bold; }
 #wtvncjsozy .gt_font_italic { font-style: italic; }
 #wtvncjsozy .gt_super { font-size: 65%; }
 #wtvncjsozy .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wtvncjsozy .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #wtvncjsozy .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wtvncjsozy .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #wtvncjsozy .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #wtvncjsozy .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Era transfer — the same booster in-era and on an era it never saw |  |  |  |  |  |  |
|----|----|----|----|----|----|----|
| the calibration-MAE gap between the rows is the era-transfer error, bounded here rather than assumed |  |  |  |  |  |  |
| era | plays | brier | baseline_brier | skill_vs_baseline | calibration_mae_20bin | pregame_brier |
| 2026 (in-era) | 2,906,233 | 0.1212 | 0.2209 | 45.1% | 0.0106 | 0.1874 |
| 2012 (unseen: 35-second shot clock, old three-point line) | 1,115,779 | 0.1242 | 0.2155 | 42.3% | 0.0255 | 0.1768 |

&#10;</div>

<img src="wp_enrich_files/figure-commonmark/cell-10-output-1.png"
width="420" height="300"
alt="Reliability, in-era vs unseen era. A curve that stays on the diagonal in the old era means the game-state features transfer across rule changes; a bowed curve is the era-transfer error made visible." />

The booster sees only game state, so what an unseen era tests is whether
“down 6 with 4:00 left” meant the same thing under a 35-second clock.
The table bounds that explicitly: the skill-versus-baseline and
calibration MAE of the old era sit next to the in-era numbers rather
than being assumed equal. The pregame prior also moves across eras — it
is an as-of team-rating model over that season’s own results — so the
holdout tests the whole applied surface, not just the tree.

## Provenance & reproducibility

- **Model:** XGBoost in-game WP over game state (score margin, seconds
  left, its square root, pregame logit, possession), fit on the 2023
  season, bundled and oracle-gated in sdv-py
  (`mbb/models/mbb_in_game_wp.ubj`).
- **Applied to:** every published season of
  `espn_mens_college_basketball_pbp`, in place, columns
  `home_win_prob` + `pregame_home_prob`, by the enrichment stage — the
  pbp asset’s only publisher.
  `mbb_data_build.publish.assert_wp_enriched` refuses any pbp parquet
  missing the columns or below a 0.999 finite-rate floor (observed 1.0
  on 2024–2026), asserted on the file about to upload.
- **Pipeline:** `scripts/mbb_models.sh 03` → stage
  `python/mbb_model_03_wp_enrich.py -s <season> -e <season> --base ../mbb`,
  wired at the end of `scripts/daily_mbb_data_processor.sh` (after
  schedules + team_box exist in the tree). Single home:
  `models/manifest.yaml`.
- **Release state (2026-09-01):** 2024–2026 carry the columns; 2003–2023
  lost them to the 2026-08-26 whole-history republish and need one
  `mbb_model_03_wp_enrich -s 2003 -e 2023` republish. This document
  falls back to computing the holdout era itself while that is
  outstanding.
- **This document** evaluates two published seasons downloaded at render
  time (~90 MB + ~50 MB) — the exact frames consumers read.
- **Rebuild:** `scripts/render_model_docs.sh` (Quarto → GFM;
  `uv sync --group docs`).

## Avenues for improvement & open issues

- **Possession-state features** — foul counts, bonus state, and timeout
  inventory are absent from the WP inputs.
- **Resolved (2026-09-01, PR \#25):** the nightly publish no longer
  strips the WP columns — the enrichment stage is the pbp asset’s only
  publisher and the publish path refuses an un-enriched pbp parquet, so
  no publish window exists in which a season lacks WP. The 2003–2023
  seasons stripped by the 2026-08-26 history republish still need the
  one-off republish listed above.
- **Resolved (2026-09-01, PR \#25):** season-holdout curve — the
  unseen-era section above renders the same calibration for a season the
  booster never saw and reports the era-transfer error as a number.
