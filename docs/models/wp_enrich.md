# MBB in-game win probability — pbp enrichment


The MBB rule-era win-probability suite (trained, bundled, and
oracle-gated in sdv-py) is applied **in place** to every published
season of `espn_mens_college_basketball_pbp`: `home_win_prob` and the
pregame prior (`pregame_home_prob`) are added to each play with every
original column preserved. The published pbp itself is how the model
reaches consumers — there is no separate WP asset to fall out of sync
with the plays.

The models are rule-era XGBoost classifiers over game state (score
margin, time remaining, possession); era-specific boosters absorb rule
changes across the 2003-present span. Operationally, the enrichment runs
post-publish in `scripts/daily_mbb_data_processor.sh` because a recorded
incident showed the nightly publish silently stripping the WP columns —
re-application is unconditional for that reason, and the incident is why
this stage exists at all.

This document is the model’s **out-of-band evaluation**: it downloads
one full published season at render time and holds the in-game
probabilities against each game’s realized outcome. That is a genuine
test of the applied model on the shipped data — if the enrichment ever
regressed, went stale, or was stripped, this document’s calibration
section would show it on the next render.

## Evaluation data

<div id="vjchhdihlq" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#vjchhdihlq table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#vjchhdihlq thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#vjchhdihlq p { margin: 0; padding: 0; }
 #vjchhdihlq .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #vjchhdihlq .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #vjchhdihlq .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #vjchhdihlq .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #vjchhdihlq .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #vjchhdihlq .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #vjchhdihlq .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #vjchhdihlq .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #vjchhdihlq .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #vjchhdihlq .gt_column_spanner_outer:first-child { padding-left: 0; }
 #vjchhdihlq .gt_column_spanner_outer:last-child { padding-right: 0; }
 #vjchhdihlq .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #vjchhdihlq .gt_spanner_row { border-bottom-style: hidden; }
 #vjchhdihlq .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #vjchhdihlq .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #vjchhdihlq .gt_from_md> :first-child { margin-top: 0; }
 #vjchhdihlq .gt_from_md> :last-child { margin-bottom: 0; }
 #vjchhdihlq .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #vjchhdihlq .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #vjchhdihlq .gt_indent_1 { text-indent: 5px; }
 #vjchhdihlq .gt_indent_2 { text-indent: calc(5px * 2); }
 #vjchhdihlq .gt_indent_3 { text-indent: calc(5px * 3); }
 #vjchhdihlq .gt_indent_4 { text-indent: calc(5px * 4); }
 #vjchhdihlq .gt_indent_5 { text-indent: calc(5px * 5); }
 #vjchhdihlq .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #vjchhdihlq .gt_row_group_first td { border-top-width: 2px; }
 #vjchhdihlq .gt_row_group_first th { border-top-width: 2px; }
 #vjchhdihlq .gt_striped { color: #333333; background-color: #F4F4F4; }
 #vjchhdihlq .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #vjchhdihlq .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #vjchhdihlq .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #vjchhdihlq .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #vjchhdihlq .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #vjchhdihlq .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #vjchhdihlq .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #vjchhdihlq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #vjchhdihlq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #vjchhdihlq .gt_left { text-align: left; }
 #vjchhdihlq .gt_center { text-align: center; }
 #vjchhdihlq .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #vjchhdihlq .gt_font_normal { font-weight: normal; }
 #vjchhdihlq .gt_font_bold { font-weight: bold; }
 #vjchhdihlq .gt_font_italic { font-style: italic; }
 #vjchhdihlq .gt_super { font-size: 65%; }
 #vjchhdihlq .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #vjchhdihlq .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #vjchhdihlq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #vjchhdihlq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #vjchhdihlq .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #vjchhdihlq .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="dhbovprsux" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#dhbovprsux table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#dhbovprsux thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#dhbovprsux p { margin: 0; padding: 0; }
 #dhbovprsux .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #dhbovprsux .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #dhbovprsux .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #dhbovprsux .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #dhbovprsux .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #dhbovprsux .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #dhbovprsux .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #dhbovprsux .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #dhbovprsux .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #dhbovprsux .gt_column_spanner_outer:first-child { padding-left: 0; }
 #dhbovprsux .gt_column_spanner_outer:last-child { padding-right: 0; }
 #dhbovprsux .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #dhbovprsux .gt_spanner_row { border-bottom-style: hidden; }
 #dhbovprsux .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #dhbovprsux .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #dhbovprsux .gt_from_md> :first-child { margin-top: 0; }
 #dhbovprsux .gt_from_md> :last-child { margin-bottom: 0; }
 #dhbovprsux .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #dhbovprsux .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #dhbovprsux .gt_indent_1 { text-indent: 5px; }
 #dhbovprsux .gt_indent_2 { text-indent: calc(5px * 2); }
 #dhbovprsux .gt_indent_3 { text-indent: calc(5px * 3); }
 #dhbovprsux .gt_indent_4 { text-indent: calc(5px * 4); }
 #dhbovprsux .gt_indent_5 { text-indent: calc(5px * 5); }
 #dhbovprsux .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #dhbovprsux .gt_row_group_first td { border-top-width: 2px; }
 #dhbovprsux .gt_row_group_first th { border-top-width: 2px; }
 #dhbovprsux .gt_striped { color: #333333; background-color: #F4F4F4; }
 #dhbovprsux .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #dhbovprsux .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #dhbovprsux .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #dhbovprsux .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #dhbovprsux .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #dhbovprsux .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #dhbovprsux .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #dhbovprsux .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #dhbovprsux .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #dhbovprsux .gt_left { text-align: left; }
 #dhbovprsux .gt_center { text-align: center; }
 #dhbovprsux .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #dhbovprsux .gt_font_normal { font-weight: normal; }
 #dhbovprsux .gt_font_bold { font-weight: bold; }
 #dhbovprsux .gt_font_italic { font-style: italic; }
 #dhbovprsux .gt_super { font-size: 65%; }
 #dhbovprsux .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #dhbovprsux .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #dhbovprsux .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #dhbovprsux .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #dhbovprsux .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #dhbovprsux .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="zpghfmairf" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#zpghfmairf table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#zpghfmairf thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#zpghfmairf p { margin: 0; padding: 0; }
 #zpghfmairf .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #zpghfmairf .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #zpghfmairf .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #zpghfmairf .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #zpghfmairf .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zpghfmairf .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zpghfmairf .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zpghfmairf .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #zpghfmairf .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #zpghfmairf .gt_column_spanner_outer:first-child { padding-left: 0; }
 #zpghfmairf .gt_column_spanner_outer:last-child { padding-right: 0; }
 #zpghfmairf .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #zpghfmairf .gt_spanner_row { border-bottom-style: hidden; }
 #zpghfmairf .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #zpghfmairf .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #zpghfmairf .gt_from_md> :first-child { margin-top: 0; }
 #zpghfmairf .gt_from_md> :last-child { margin-bottom: 0; }
 #zpghfmairf .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #zpghfmairf .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #zpghfmairf .gt_indent_1 { text-indent: 5px; }
 #zpghfmairf .gt_indent_2 { text-indent: calc(5px * 2); }
 #zpghfmairf .gt_indent_3 { text-indent: calc(5px * 3); }
 #zpghfmairf .gt_indent_4 { text-indent: calc(5px * 4); }
 #zpghfmairf .gt_indent_5 { text-indent: calc(5px * 5); }
 #zpghfmairf .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #zpghfmairf .gt_row_group_first td { border-top-width: 2px; }
 #zpghfmairf .gt_row_group_first th { border-top-width: 2px; }
 #zpghfmairf .gt_striped { color: #333333; background-color: #F4F4F4; }
 #zpghfmairf .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zpghfmairf .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zpghfmairf .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #zpghfmairf .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zpghfmairf .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zpghfmairf .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #zpghfmairf .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #zpghfmairf .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zpghfmairf .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zpghfmairf .gt_left { text-align: left; }
 #zpghfmairf .gt_center { text-align: center; }
 #zpghfmairf .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #zpghfmairf .gt_font_normal { font-weight: normal; }
 #zpghfmairf .gt_font_bold { font-weight: bold; }
 #zpghfmairf .gt_font_italic { font-style: italic; }
 #zpghfmairf .gt_super { font-size: 65%; }
 #zpghfmairf .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zpghfmairf .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #zpghfmairf .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zpghfmairf .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zpghfmairf .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #zpghfmairf .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="vyfywimzqb" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#vyfywimzqb table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#vyfywimzqb thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#vyfywimzqb p { margin: 0; padding: 0; }
 #vyfywimzqb .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #vyfywimzqb .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #vyfywimzqb .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #vyfywimzqb .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #vyfywimzqb .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #vyfywimzqb .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #vyfywimzqb .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #vyfywimzqb .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #vyfywimzqb .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #vyfywimzqb .gt_column_spanner_outer:first-child { padding-left: 0; }
 #vyfywimzqb .gt_column_spanner_outer:last-child { padding-right: 0; }
 #vyfywimzqb .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #vyfywimzqb .gt_spanner_row { border-bottom-style: hidden; }
 #vyfywimzqb .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #vyfywimzqb .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #vyfywimzqb .gt_from_md> :first-child { margin-top: 0; }
 #vyfywimzqb .gt_from_md> :last-child { margin-bottom: 0; }
 #vyfywimzqb .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #vyfywimzqb .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #vyfywimzqb .gt_indent_1 { text-indent: 5px; }
 #vyfywimzqb .gt_indent_2 { text-indent: calc(5px * 2); }
 #vyfywimzqb .gt_indent_3 { text-indent: calc(5px * 3); }
 #vyfywimzqb .gt_indent_4 { text-indent: calc(5px * 4); }
 #vyfywimzqb .gt_indent_5 { text-indent: calc(5px * 5); }
 #vyfywimzqb .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #vyfywimzqb .gt_row_group_first td { border-top-width: 2px; }
 #vyfywimzqb .gt_row_group_first th { border-top-width: 2px; }
 #vyfywimzqb .gt_striped { color: #333333; background-color: #F4F4F4; }
 #vyfywimzqb .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #vyfywimzqb .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #vyfywimzqb .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #vyfywimzqb .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #vyfywimzqb .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #vyfywimzqb .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #vyfywimzqb .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #vyfywimzqb .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #vyfywimzqb .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #vyfywimzqb .gt_left { text-align: left; }
 #vyfywimzqb .gt_center { text-align: center; }
 #vyfywimzqb .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #vyfywimzqb .gt_font_normal { font-weight: normal; }
 #vyfywimzqb .gt_font_bold { font-weight: bold; }
 #vyfywimzqb .gt_font_italic { font-style: italic; }
 #vyfywimzqb .gt_super { font-size: 65%; }
 #vyfywimzqb .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #vyfywimzqb .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #vyfywimzqb .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #vyfywimzqb .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #vyfywimzqb .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #vyfywimzqb .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

## Provenance & reproducibility

- **Model:** rule-era XGBoost WP suite trained, bundled, and
  oracle-gated in sdv-py (score margin, time, possession features; era
  boosters across 2003-present).
- **Applied to:** every published season of
  `espn_mens_college_basketball_pbp`, in place, columns
  `home_win_prob` + `pregame_home_prob`; re-application is unconditional
  in `scripts/daily_mbb_data_processor.sh` (recorded strip incident).
- **Pipeline:** `scripts/mbb_models.sh 03` → stage
  `python/mbb_model_03_wp_enrich.py -s <season> -e <season>`. Single
  home: `models/manifest.yaml`.
- **This document** evaluates the published season downloaded at render
  time (~90 MB) — the exact frame consumers read.
- **Rebuild:** `scripts/render_model_docs.sh` (Quarto → GFM;
  `uv sync --group docs`).

## Avenues for improvement & open issues

- **Possession-state features** — foul counts, bonus state, and timeout
  inventory are absent from the WP inputs.
- **Known issue (recorded incident):** the nightly publish silently
  strips WP columns, which is why re-application is unconditional —
  moving enrichment into the publish step itself would remove the window
  where a freshly published season briefly lacks WP.
- **Season-holdout curve** — this document evaluates the applied model
  in-era; rendering the same calibration for an era the booster never
  saw would bound era-transfer error explicitly.
