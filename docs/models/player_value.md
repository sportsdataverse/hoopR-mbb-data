# MBB player value — box Plus/Minus


Per-player box Plus/Minus publishes on the `mbb_player_value` tag
(`box_obpm` / `box_dbpm` / `box_bpm`): a box-score value model over the
published player/team season stats, sharing the design of the MBB
player-value spine in sdv-py (oracle-gated where trained). Box-score
features are regressed onto team-level results to apportion value;
offensive and defensive components are estimated separately and summed.
It is compute-on-demand — every run recomputes from the current
published season assets, so a data correction upstream flows through on
the next publish, and each publish writes a card sidecar.

Box Plus/Minus and on/off RAPM (the `ncaa_mbb_rapm` model in the NCAA
hoops repos) deliberately measure different things: BPM sees only what
reaches the box score and is therefore stable at small samples but blind
to screening, defensive attention, and everything else the box misses;
RAPM sees all of it but drowns in noise for low-minute players. The two
are cross-references, not substitutes — the natural hybrid (an SPM-prior
RAPM, as the NBA impact suite builds) is the catalogued next step.

Everything below is computed at render time from the published release
assets — the exact frames consumers download.

## Training data

<div id="qsdnvwekjw" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#qsdnvwekjw table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#qsdnvwekjw thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#qsdnvwekjw p { margin: 0; padding: 0; }
 #qsdnvwekjw .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #qsdnvwekjw .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #qsdnvwekjw .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #qsdnvwekjw .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #qsdnvwekjw .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #qsdnvwekjw .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qsdnvwekjw .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #qsdnvwekjw .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #qsdnvwekjw .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #qsdnvwekjw .gt_column_spanner_outer:first-child { padding-left: 0; }
 #qsdnvwekjw .gt_column_spanner_outer:last-child { padding-right: 0; }
 #qsdnvwekjw .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #qsdnvwekjw .gt_spanner_row { border-bottom-style: hidden; }
 #qsdnvwekjw .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #qsdnvwekjw .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #qsdnvwekjw .gt_from_md> :first-child { margin-top: 0; }
 #qsdnvwekjw .gt_from_md> :last-child { margin-bottom: 0; }
 #qsdnvwekjw .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #qsdnvwekjw .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #qsdnvwekjw .gt_indent_1 { text-indent: 5px; }
 #qsdnvwekjw .gt_indent_2 { text-indent: calc(5px * 2); }
 #qsdnvwekjw .gt_indent_3 { text-indent: calc(5px * 3); }
 #qsdnvwekjw .gt_indent_4 { text-indent: calc(5px * 4); }
 #qsdnvwekjw .gt_indent_5 { text-indent: calc(5px * 5); }
 #qsdnvwekjw .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #qsdnvwekjw .gt_row_group_first td { border-top-width: 2px; }
 #qsdnvwekjw .gt_row_group_first th { border-top-width: 2px; }
 #qsdnvwekjw .gt_striped { color: #333333; background-color: #F4F4F4; }
 #qsdnvwekjw .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qsdnvwekjw .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #qsdnvwekjw .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #qsdnvwekjw .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qsdnvwekjw .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #qsdnvwekjw .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #qsdnvwekjw .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #qsdnvwekjw .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qsdnvwekjw .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #qsdnvwekjw .gt_left { text-align: left; }
 #qsdnvwekjw .gt_center { text-align: center; }
 #qsdnvwekjw .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #qsdnvwekjw .gt_font_normal { font-weight: normal; }
 #qsdnvwekjw .gt_font_bold { font-weight: bold; }
 #qsdnvwekjw .gt_font_italic { font-style: italic; }
 #qsdnvwekjw .gt_super { font-size: 65%; }
 #qsdnvwekjw .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qsdnvwekjw .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #qsdnvwekjw .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qsdnvwekjw .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #qsdnvwekjw .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #qsdnvwekjw .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Published mbb_player_value assets, by season |  |  |  |
|----|----|----|----|
| computed at render time from the release |  |  |  |
| season | players | total_minutes | players_300min |
| 2020 | 8770 | 2,324,878 | 2915 |
| 2021 | 6840 | 1,726,023 | 2477 |
| 2022 | 9364 | 2,404,864 | 2971 |
| 2023 | 9717 | 2,509,140 | 3024 |
| 2024 | 9838 | 2,516,161 | 3056 |
| 2025 | 9805 | 2,535,805 | 3065 |
| 2026 | 9990 | 2,539,283 | 3114 |

&#10;</div>

## Exploratory data analysis

<img src="player_value_files/figure-commonmark/cell-4-output-1.png"
width="420" height="300"
alt="Box BPM distribution, latest season — the full player pool vs the ≥300-minute pool. The unfiltered frame carries heavy low-minute noise by design." />

<img src="player_value_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300"
alt="Minutes vs |BPM|: with no shrinkage prior, extreme values concentrate at LOW minutes — the reason consumers must filter." />

<img src="player_value_files/figure-commonmark/cell-6-output-1.png"
width="420" height="300"
alt="Offense vs defense components, ≥300 minutes, latest season." />

The minutes-vs-\|BPM\| funnel is this model’s most important
consumer-facing fact, shown rather than footnoted: the published frame
enforces **no minutes floor**, so the most extreme values in the file
belong to 20-minute seasons. Every table below applies a floor;
consumers must too.

## Attribution

The model is a linear apportionment of team results onto box-score
features, so the published O/D columns are its native attribution — the
scatter above is the decomposition. The fitted coefficient vector lives
with the engine in sdv-py (oracle-gated where trained) rather than in
the published asset; surfacing it alongside the release is listed in the
avenues below.

## Evaluation

<div id="ysammluhfm" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ysammluhfm table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ysammluhfm thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ysammluhfm p { margin: 0; padding: 0; }
 #ysammluhfm .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ysammluhfm .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ysammluhfm .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ysammluhfm .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ysammluhfm .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ysammluhfm .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ysammluhfm .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ysammluhfm .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ysammluhfm .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ysammluhfm .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ysammluhfm .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ysammluhfm .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ysammluhfm .gt_spanner_row { border-bottom-style: hidden; }
 #ysammluhfm .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ysammluhfm .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ysammluhfm .gt_from_md> :first-child { margin-top: 0; }
 #ysammluhfm .gt_from_md> :last-child { margin-bottom: 0; }
 #ysammluhfm .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ysammluhfm .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ysammluhfm .gt_indent_1 { text-indent: 5px; }
 #ysammluhfm .gt_indent_2 { text-indent: calc(5px * 2); }
 #ysammluhfm .gt_indent_3 { text-indent: calc(5px * 3); }
 #ysammluhfm .gt_indent_4 { text-indent: calc(5px * 4); }
 #ysammluhfm .gt_indent_5 { text-indent: calc(5px * 5); }
 #ysammluhfm .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ysammluhfm .gt_row_group_first td { border-top-width: 2px; }
 #ysammluhfm .gt_row_group_first th { border-top-width: 2px; }
 #ysammluhfm .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ysammluhfm .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ysammluhfm .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ysammluhfm .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ysammluhfm .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ysammluhfm .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ysammluhfm .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ysammluhfm .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ysammluhfm .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ysammluhfm .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ysammluhfm .gt_left { text-align: left; }
 #ysammluhfm .gt_center { text-align: center; }
 #ysammluhfm .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ysammluhfm .gt_font_normal { font-weight: normal; }
 #ysammluhfm .gt_font_bold { font-weight: bold; }
 #ysammluhfm .gt_font_italic { font-style: italic; }
 #ysammluhfm .gt_super { font-size: 65%; }
 #ysammluhfm .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ysammluhfm .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ysammluhfm .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ysammluhfm .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ysammluhfm .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ysammluhfm .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Published-asset checks |  |  |
|----|----|----|
| YoY reliability is the box model's core virtue; a near-zero O/D correlation means the components carry distinct information |  |  |
| check | pairs | pearson |
| box BPM year-over-year (same player, ≥300 min both seasons) | 10110 | 0.750 |
| corr(box_obpm, box_dbpm) — 2026, ≥300 min | 3114 | 0.151 |

&#10;</div>

A box model’s justification is exactly this reliability: with
roster-level churn as violent as college basketball’s, a player metric
that persists season-over-season for returning players is measuring the
player. The engine’s own oracle gates (against the sdv-py player-value
spine’s references) run where the model is trained.

## Results

<div id="qbpivczajy" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#qbpivczajy table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#qbpivczajy thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#qbpivczajy p { margin: 0; padding: 0; }
 #qbpivczajy .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #qbpivczajy .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #qbpivczajy .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #qbpivczajy .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #qbpivczajy .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #qbpivczajy .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qbpivczajy .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #qbpivczajy .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #qbpivczajy .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #qbpivczajy .gt_column_spanner_outer:first-child { padding-left: 0; }
 #qbpivczajy .gt_column_spanner_outer:last-child { padding-right: 0; }
 #qbpivczajy .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #qbpivczajy .gt_spanner_row { border-bottom-style: hidden; }
 #qbpivczajy .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #qbpivczajy .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #qbpivczajy .gt_from_md> :first-child { margin-top: 0; }
 #qbpivczajy .gt_from_md> :last-child { margin-bottom: 0; }
 #qbpivczajy .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #qbpivczajy .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #qbpivczajy .gt_indent_1 { text-indent: 5px; }
 #qbpivczajy .gt_indent_2 { text-indent: calc(5px * 2); }
 #qbpivczajy .gt_indent_3 { text-indent: calc(5px * 3); }
 #qbpivczajy .gt_indent_4 { text-indent: calc(5px * 4); }
 #qbpivczajy .gt_indent_5 { text-indent: calc(5px * 5); }
 #qbpivczajy .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #qbpivczajy .gt_row_group_first td { border-top-width: 2px; }
 #qbpivczajy .gt_row_group_first th { border-top-width: 2px; }
 #qbpivczajy .gt_striped { color: #333333; background-color: #F4F4F4; }
 #qbpivczajy .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qbpivczajy .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #qbpivczajy .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #qbpivczajy .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qbpivczajy .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #qbpivczajy .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #qbpivczajy .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #qbpivczajy .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qbpivczajy .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #qbpivczajy .gt_left { text-align: left; }
 #qbpivczajy .gt_center { text-align: center; }
 #qbpivczajy .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #qbpivczajy .gt_font_normal { font-weight: normal; }
 #qbpivczajy .gt_font_bold { font-weight: bold; }
 #qbpivczajy .gt_font_italic { font-style: italic; }
 #qbpivczajy .gt_super { font-size: 65%; }
 #qbpivczajy .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qbpivczajy .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #qbpivczajy .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qbpivczajy .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #qbpivczajy .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #qbpivczajy .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Top 15 box BPM — 2026 (min 300 minutes) |  |  |  |  |  |  |
|----|----|----|----|----|----|----|
|  | Player | Team | Min | O-BPM | D-BPM | BPM |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5105555.png"
height="40" /> | Tobe Awaka | Arizona Wildcats | 809 | 7.33 | 6.40 | 13.73 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5174973.png"
height="40" /> | Oscar Cluff | Purdue Boilermakers | 964 | 10.08 | 3.29 | 13.37 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/4873153.png"
height="40" /> | Morez Johnson Jr. | Michigan Wolverines | 1,005 | 7.16 | 5.82 | 12.98 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5041935.png"
height="40" /> | Cameron Boozer | Duke Blue Devils | 1,274 | 10.45 | 2.49 | 12.94 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5174954.png"
height="40" /> | Motiejus Krivas | Arizona Wildcats | 984 | 8.77 | 4.05 | 12.81 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5175737.png"
height="40" /> | Yaxel Lendeborg | Michigan Wolverines | 1,210 | 9.59 | 3.12 | 12.71 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5176274.png"
height="40" /> | Kalifa Sakho | Houston Cougars | 435 | 5.48 | 7.10 | 12.58 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5174971.png"
height="40" /> | Rueben Chinyelu | Florida Gators | 856 | 5.98 | 6.53 | 12.51 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5108992.png"
height="40" /> | Micah Handlogten | Florida Gators | 504 | 4.38 | 8.01 | 12.39 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/4873209.png"
height="40" /> | Patrick Ngongba II | Duke Blue Devils | 702 | 8.35 | 3.73 | 12.08 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5105647.png"
height="40" /> | Keba Keita | BYU Cougars | 747 | 7.38 | 4.58 | 11.96 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5106261.png"
height="40" /> | Ernest Udeh Jr. | Miami Hurricanes | 933 | 6.64 | 5.09 | 11.73 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5174983.png"
height="40" /> | Aday Mara | Michigan Wolverines | 935 | 7.73 | 3.99 | 11.72 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5105337.png"
height="40" /> | Maliq Brown | Duke Blue Devils | 769 | 2.67 | 8.72 | 11.39 |
| <img
src="https://a.espncdn.com/i/headshots/mens-college-basketball/players/full/5311967.png"
height="40" /> | Sananda Fru | Louisville Cardinals | 770 | 7.11 | 4.11 | 11.22 |

&#10;</div>

## Provenance & reproducibility

- **Computed from:** this repository’s published player/team season
  stats for the seasons in the corpus table; recomputed in full on every
  run (compute-on-demand — no fitted artifact is stored).
- **Engine:** the MBB player-value spine in sdv-py (oracle-gated where
  trained); O/D estimated separately and summed.
- **Pipeline:** `scripts/mbb_models.sh 02` → stage
  `python/mbb_model_02_player_value.py`; card sidecar
  [`mbb_models_eval_card.json`](mbb_models_eval_card.json). Single home:
  `models/manifest.yaml`.
- **Rebuild this document:** `scripts/render_model_docs.sh` (Quarto →
  GFM; `uv sync --group docs`). Requires network for the release
  download and the ESPN headshot CDN.

## Avenues for improvement & open issues

- **Blend with on/off** — box Plus/Minus and the league-wide RAPM
  measure different things; a stabilized hybrid (SPM-prior RAPM, as the
  NBA impact suite does) is the natural next step.
- **Ship the coefficient vector** — publishing the fitted coefficients
  (or a per-retrain meta sidecar) would let this document show real
  coefficient importance instead of pointing at the engine.
- **Known issue:** no minutes floor is enforced in the published frame —
  consumers must filter low-minute noise themselves (the funnel figure
  above is the demonstration).
