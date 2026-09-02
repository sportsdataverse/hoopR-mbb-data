# MBB player value — box Plus/Minus


Per-player box Plus/Minus publishes on the `mbb_player_value` tag
(`box_obpm` / `box_dbpm` / `box_bpm`): a box-score value model over the
published player/team season stats, sharing the design of the MBB
player-value spine in sdv-py (oracle-gated where trained). Per-100 box
features are standardized and scored through ridge coefficients fit at
the team level, then a uniform team adjustment makes each team’s
minutes-weighted player scores sum to its adjusted efficiency margin;
offensive and defensive components are estimated separately and summed.
It is compute-on-demand — every run recomputes from the current
published season assets, so a data correction upstream flows through on
the next publish, and each publish writes a card sidecar plus the fitted
coefficient vector.

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

<div id="geztepklud" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#geztepklud table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#geztepklud thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#geztepklud p { margin: 0; padding: 0; }
 #geztepklud .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #geztepklud .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #geztepklud .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #geztepklud .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #geztepklud .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #geztepklud .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #geztepklud .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #geztepklud .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #geztepklud .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #geztepklud .gt_column_spanner_outer:first-child { padding-left: 0; }
 #geztepklud .gt_column_spanner_outer:last-child { padding-right: 0; }
 #geztepklud .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #geztepklud .gt_spanner_row { border-bottom-style: hidden; }
 #geztepklud .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #geztepklud .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #geztepklud .gt_from_md> :first-child { margin-top: 0; }
 #geztepklud .gt_from_md> :last-child { margin-bottom: 0; }
 #geztepklud .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #geztepklud .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #geztepklud .gt_indent_1 { text-indent: 5px; }
 #geztepklud .gt_indent_2 { text-indent: calc(5px * 2); }
 #geztepklud .gt_indent_3 { text-indent: calc(5px * 3); }
 #geztepklud .gt_indent_4 { text-indent: calc(5px * 4); }
 #geztepklud .gt_indent_5 { text-indent: calc(5px * 5); }
 #geztepklud .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #geztepklud .gt_row_group_first td { border-top-width: 2px; }
 #geztepklud .gt_row_group_first th { border-top-width: 2px; }
 #geztepklud .gt_striped { color: #333333; background-color: #F4F4F4; }
 #geztepklud .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #geztepklud .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #geztepklud .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #geztepklud .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #geztepklud .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #geztepklud .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #geztepklud .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #geztepklud .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #geztepklud .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #geztepklud .gt_left { text-align: left; }
 #geztepklud .gt_center { text-align: center; }
 #geztepklud .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #geztepklud .gt_font_normal { font-weight: normal; }
 #geztepklud .gt_font_bold { font-weight: bold; }
 #geztepklud .gt_font_italic { font-style: italic; }
 #geztepklud .gt_super { font-size: 65%; }
 #geztepklud .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #geztepklud .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #geztepklud .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #geztepklud .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #geztepklud .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #geztepklud .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Published mbb_player_value assets, by season |  |  |  |
|----|----|----|----|
| computed at render time from the release; qualified = min \>= 300 |  |  |  |
| season | players | total_minutes | qualified_players |
| 2014 | 8161 | 2,393,564 | 2911 |
| 2015 | 8301 | 2,391,127 | 2909 |
| 2016 | 8438 | 2,373,935 | 2912 |
| 2017 | 8674 | 2,388,044 | 2907 |
| 2018 | 8724 | 2,421,452 | 2940 |
| 2019 | 8649 | 2,432,949 | 2940 |
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
alt="Box BPM distribution, latest season — the full player pool vs the qualified pool. The unfiltered frame carries heavy low-minute noise by design; the flag marks it, it never removes it." />

<img src="player_value_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300"
alt="Minutes vs |BPM|: with no shrinkage prior, extreme values concentrate at LOW minutes. The vertical line is the qualified floor." />

<div id="apxyincnif" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#apxyincnif table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#apxyincnif thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#apxyincnif p { margin: 0; padding: 0; }
 #apxyincnif .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #apxyincnif .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #apxyincnif .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #apxyincnif .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #apxyincnif .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #apxyincnif .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #apxyincnif .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #apxyincnif .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #apxyincnif .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #apxyincnif .gt_column_spanner_outer:first-child { padding-left: 0; }
 #apxyincnif .gt_column_spanner_outer:last-child { padding-right: 0; }
 #apxyincnif .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #apxyincnif .gt_spanner_row { border-bottom-style: hidden; }
 #apxyincnif .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #apxyincnif .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #apxyincnif .gt_from_md> :first-child { margin-top: 0; }
 #apxyincnif .gt_from_md> :last-child { margin-bottom: 0; }
 #apxyincnif .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #apxyincnif .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #apxyincnif .gt_indent_1 { text-indent: 5px; }
 #apxyincnif .gt_indent_2 { text-indent: calc(5px * 2); }
 #apxyincnif .gt_indent_3 { text-indent: calc(5px * 3); }
 #apxyincnif .gt_indent_4 { text-indent: calc(5px * 4); }
 #apxyincnif .gt_indent_5 { text-indent: calc(5px * 5); }
 #apxyincnif .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #apxyincnif .gt_row_group_first td { border-top-width: 2px; }
 #apxyincnif .gt_row_group_first th { border-top-width: 2px; }
 #apxyincnif .gt_striped { color: #333333; background-color: #F4F4F4; }
 #apxyincnif .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #apxyincnif .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #apxyincnif .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #apxyincnif .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #apxyincnif .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #apxyincnif .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #apxyincnif .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #apxyincnif .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #apxyincnif .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #apxyincnif .gt_left { text-align: left; }
 #apxyincnif .gt_center { text-align: center; }
 #apxyincnif .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #apxyincnif .gt_font_normal { font-weight: normal; }
 #apxyincnif .gt_font_bold { font-weight: bold; }
 #apxyincnif .gt_font_italic { font-style: italic; }
 #apxyincnif .gt_super { font-size: 65%; }
 #apxyincnif .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #apxyincnif .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #apxyincnif .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #apxyincnif .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #apxyincnif .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #apxyincnif .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| How the qualified floor was set — sd(box_bpm) by minutes bin, all published seasons |  |  |  |  |
|----|----|----|----|----|
| the floor is the first bin whose sd sits within 10% of the 600–800 plateau (3.73); every table below uses the flag |  |  |  |  |
| minutes_bin | players | sd_box_bpm | abs_bpm_p99 | vs_600_800_plateau |
| 0-25 | 43,386 | 7.17 | 18.80 | 92% |
| 25-50 | 15,429 | 5.72 | 14.97 | 53% |
| 50-75 | 4,868 | 5.15 | 13.68 | 38% |
| 75-100 | 2,545 | 5.07 | 13.79 | 36% |
| 100-150 | 3,396 | 4.78 | 13.01 | 28% |
| 150-200 | 2,625 | 4.59 | 12.13 | 23% |
| 200-250 | 2,504 | 4.30 | 11.85 | 15% |
| 250-300 | 2,431 | 4.23 | 11.07 | 13% |
| 300-350 | 2,449 | 4.08 | 10.64 | 9% |
| 350-400 | 2,247 | 3.93 | 9.94 | 5% |
| 400-500 | 4,455 | 3.95 | 10.23 | 6% |
| 500-600 | 4,642 | 3.89 | 9.84 | 4% |
| 600-800 | 9,557 | 3.73 | 9.70 | −0% |
| 800-1000 | 9,080 | 3.61 | 9.70 | −3% |
| 1000-1400 | 5,657 | 3.43 | 9.37 | −8% |

&#10;</div>

Flag source in this render: derived at render time as min \>= 300 (the
published frames predate the flag).

<img src="player_value_files/figure-commonmark/cell-7-output-1.png"
width="420" height="300"
alt="Offense vs defense components, qualified players, latest season." />

The minutes-vs-\|BPM\| funnel is this model’s most important
consumer-facing fact, shown rather than footnoted: the published frame
keeps **every** player, so the most extreme values in the file belong to
20-minute seasons. The additive `qualified` flag encodes the floor the
funnel itself justifies — the bin where the spread of BPM stops
shrinking — so a consumer filters with one boolean instead of
re-deriving a threshold. The engine’s own fit floor (`min_minutes` in
the artifact) governs only the team-sum weights and is lower; the two
floors answer different questions.

## Coefficient importance

Coefficients from the bundled sdv-py artifact (the sidecar is not on the
release yet); fit on seasons \[2025, 2026\] (ridge λ offense 300.0,
defense 100.0; z-clip 4.0; fit floor 150.0 minutes).

<div id="xbwrvrhoft" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#xbwrvrhoft table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#xbwrvrhoft thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#xbwrvrhoft p { margin: 0; padding: 0; }
 #xbwrvrhoft .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #xbwrvrhoft .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #xbwrvrhoft .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #xbwrvrhoft .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #xbwrvrhoft .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #xbwrvrhoft .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xbwrvrhoft .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #xbwrvrhoft .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #xbwrvrhoft .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #xbwrvrhoft .gt_column_spanner_outer:first-child { padding-left: 0; }
 #xbwrvrhoft .gt_column_spanner_outer:last-child { padding-right: 0; }
 #xbwrvrhoft .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #xbwrvrhoft .gt_spanner_row { border-bottom-style: hidden; }
 #xbwrvrhoft .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #xbwrvrhoft .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #xbwrvrhoft .gt_from_md> :first-child { margin-top: 0; }
 #xbwrvrhoft .gt_from_md> :last-child { margin-bottom: 0; }
 #xbwrvrhoft .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #xbwrvrhoft .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #xbwrvrhoft .gt_indent_1 { text-indent: 5px; }
 #xbwrvrhoft .gt_indent_2 { text-indent: calc(5px * 2); }
 #xbwrvrhoft .gt_indent_3 { text-indent: calc(5px * 3); }
 #xbwrvrhoft .gt_indent_4 { text-indent: calc(5px * 4); }
 #xbwrvrhoft .gt_indent_5 { text-indent: calc(5px * 5); }
 #xbwrvrhoft .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #xbwrvrhoft .gt_row_group_first td { border-top-width: 2px; }
 #xbwrvrhoft .gt_row_group_first th { border-top-width: 2px; }
 #xbwrvrhoft .gt_striped { color: #333333; background-color: #F4F4F4; }
 #xbwrvrhoft .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xbwrvrhoft .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #xbwrvrhoft .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #xbwrvrhoft .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xbwrvrhoft .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #xbwrvrhoft .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #xbwrvrhoft .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #xbwrvrhoft .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xbwrvrhoft .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #xbwrvrhoft .gt_left { text-align: left; }
 #xbwrvrhoft .gt_center { text-align: center; }
 #xbwrvrhoft .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #xbwrvrhoft .gt_font_normal { font-weight: normal; }
 #xbwrvrhoft .gt_font_bold { font-weight: bold; }
 #xbwrvrhoft .gt_font_italic { font-style: italic; }
 #xbwrvrhoft .gt_super { font-size: 65%; }
 #xbwrvrhoft .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xbwrvrhoft .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #xbwrvrhoft .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xbwrvrhoft .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #xbwrvrhoft .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #xbwrvrhoft .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Fitted box-BPM coefficients (slopes per one SD of the standardized feature) |  |  |  |  |
|----|----|----|----|----|
| intercepts are team-level and absorbed by the team adjustment; \|slope\| is the BPM moved by one standard deviation of the feature |  |  |  |  |
| feature | obpm_slope | dbpm_slope | feature_mean | feature_sd |
| pts_per100 | 2.353 | −2.018 | 35.0829 | 11.1700 |
| usage | −2.397 | 1.638 | 37.3422 | 10.1187 |
| ast_per100 | 1.593 | −1.234 | 6.3922 | 3.7979 |
| ts_pct | 1.886 | 0.304 | 0.5484 | 0.0691 |
| reb_per100 | 1.200 | 0.337 | 16.1386 | 6.7105 |
| tov_pct | −1.249 | 0.174 | 0.1490 | 0.0542 |
| rim_share | −0.752 | 0.560 | 0.4025 | 0.2106 |
| blk_pct | 0.691 | −0.531 | 0.0790 | 0.1125 |
| ftr | 0.527 | −0.538 | 0.3501 | 0.1773 |
| three_share | 0.455 | −0.353 | 0.3794 | 0.2366 |
| mid_share | 0.442 | −0.301 | 0.2181 | 0.1150 |
| efg_pct | −0.564 | 0.103 | 0.5138 | 0.0763 |
| ast_pct | −0.051 | 0.458 | 0.2454 | 0.1692 |
| oreb_pct | 0.325 | −0.106 | 0.2703 | 0.1119 |
| dreb_pct | −0.325 | 0.106 | 0.7297 | 0.1119 |
| stl_pct | 0.018 | 0.280 | 0.1293 | 0.0772 |

&#10;</div>

<img src="player_value_files/figure-commonmark/cell-9-output-1.png"
width="420" height="300"
alt="Coefficient importance: BPM change per one SD of each standardized per-100 feature, offense and defense." />

Because every feature is standardized before scoring, the slopes are
directly comparable: a slope of 2 means one standard deviation of that
rate moves a player’s BPM by two points per 100 possessions. Usage and
true shooting dominate offense with opposite signs (volume is penalized
until it is efficient), and points per 100 carries the offensive load;
the defensive vector is smaller and led by the same usage/efficiency
pair with the sign flipped, with rebounding and steals the distinctly
defensive contributors. The vector is republished with every run as
`mbb_player_value_coefficients.json`, alongside the artifact’s
standardization moments, so a consumer can reproduce any player’s raw
score from the published per-100 features.

## Evaluation

<div id="pceewjonsm" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#pceewjonsm table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#pceewjonsm thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#pceewjonsm p { margin: 0; padding: 0; }
 #pceewjonsm .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #pceewjonsm .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #pceewjonsm .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #pceewjonsm .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #pceewjonsm .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #pceewjonsm .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #pceewjonsm .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #pceewjonsm .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #pceewjonsm .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #pceewjonsm .gt_column_spanner_outer:first-child { padding-left: 0; }
 #pceewjonsm .gt_column_spanner_outer:last-child { padding-right: 0; }
 #pceewjonsm .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #pceewjonsm .gt_spanner_row { border-bottom-style: hidden; }
 #pceewjonsm .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #pceewjonsm .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #pceewjonsm .gt_from_md> :first-child { margin-top: 0; }
 #pceewjonsm .gt_from_md> :last-child { margin-bottom: 0; }
 #pceewjonsm .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #pceewjonsm .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #pceewjonsm .gt_indent_1 { text-indent: 5px; }
 #pceewjonsm .gt_indent_2 { text-indent: calc(5px * 2); }
 #pceewjonsm .gt_indent_3 { text-indent: calc(5px * 3); }
 #pceewjonsm .gt_indent_4 { text-indent: calc(5px * 4); }
 #pceewjonsm .gt_indent_5 { text-indent: calc(5px * 5); }
 #pceewjonsm .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #pceewjonsm .gt_row_group_first td { border-top-width: 2px; }
 #pceewjonsm .gt_row_group_first th { border-top-width: 2px; }
 #pceewjonsm .gt_striped { color: #333333; background-color: #F4F4F4; }
 #pceewjonsm .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #pceewjonsm .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #pceewjonsm .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #pceewjonsm .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #pceewjonsm .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #pceewjonsm .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #pceewjonsm .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #pceewjonsm .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #pceewjonsm .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #pceewjonsm .gt_left { text-align: left; }
 #pceewjonsm .gt_center { text-align: center; }
 #pceewjonsm .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #pceewjonsm .gt_font_normal { font-weight: normal; }
 #pceewjonsm .gt_font_bold { font-weight: bold; }
 #pceewjonsm .gt_font_italic { font-style: italic; }
 #pceewjonsm .gt_super { font-size: 65%; }
 #pceewjonsm .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #pceewjonsm .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #pceewjonsm .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #pceewjonsm .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #pceewjonsm .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #pceewjonsm .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Published-asset checks |  |  |
|----|----|----|
| YoY reliability is the box model's core virtue; a near-zero O/D correlation means the components carry distinct information |  |  |
| check | pairs | pearson |
| box BPM year-over-year (same player, qualified both seasons) | 19414 | 0.767 |
| corr(box_obpm, box_dbpm) — 2026, qualified | 3114 | 0.151 |

&#10;</div>

A box model’s justification is exactly this reliability: with
roster-level churn as violent as college basketball’s, a player metric
that persists season-over-season for returning players is measuring the
player. The engine’s own oracle gates (against the sdv-py player-value
spine’s references) run where the model is trained.

## Results

<div id="fqnqaznvbe" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#fqnqaznvbe table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#fqnqaznvbe thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#fqnqaznvbe p { margin: 0; padding: 0; }
 #fqnqaznvbe .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #fqnqaznvbe .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #fqnqaznvbe .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #fqnqaznvbe .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #fqnqaznvbe .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #fqnqaznvbe .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #fqnqaznvbe .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #fqnqaznvbe .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #fqnqaznvbe .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #fqnqaznvbe .gt_column_spanner_outer:first-child { padding-left: 0; }
 #fqnqaznvbe .gt_column_spanner_outer:last-child { padding-right: 0; }
 #fqnqaznvbe .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #fqnqaznvbe .gt_spanner_row { border-bottom-style: hidden; }
 #fqnqaznvbe .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #fqnqaznvbe .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #fqnqaznvbe .gt_from_md> :first-child { margin-top: 0; }
 #fqnqaznvbe .gt_from_md> :last-child { margin-bottom: 0; }
 #fqnqaznvbe .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #fqnqaznvbe .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #fqnqaznvbe .gt_indent_1 { text-indent: 5px; }
 #fqnqaznvbe .gt_indent_2 { text-indent: calc(5px * 2); }
 #fqnqaznvbe .gt_indent_3 { text-indent: calc(5px * 3); }
 #fqnqaznvbe .gt_indent_4 { text-indent: calc(5px * 4); }
 #fqnqaznvbe .gt_indent_5 { text-indent: calc(5px * 5); }
 #fqnqaznvbe .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #fqnqaznvbe .gt_row_group_first td { border-top-width: 2px; }
 #fqnqaznvbe .gt_row_group_first th { border-top-width: 2px; }
 #fqnqaznvbe .gt_striped { color: #333333; background-color: #F4F4F4; }
 #fqnqaznvbe .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #fqnqaznvbe .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #fqnqaznvbe .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #fqnqaznvbe .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #fqnqaznvbe .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #fqnqaznvbe .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #fqnqaznvbe .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #fqnqaznvbe .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #fqnqaznvbe .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #fqnqaznvbe .gt_left { text-align: left; }
 #fqnqaznvbe .gt_center { text-align: center; }
 #fqnqaznvbe .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #fqnqaznvbe .gt_font_normal { font-weight: normal; }
 #fqnqaznvbe .gt_font_bold { font-weight: bold; }
 #fqnqaznvbe .gt_font_italic { font-style: italic; }
 #fqnqaznvbe .gt_super { font-size: 65%; }
 #fqnqaznvbe .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #fqnqaznvbe .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #fqnqaznvbe .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #fqnqaznvbe .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #fqnqaznvbe .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #fqnqaznvbe .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Top 15 box BPM — 2026 (qualified players) |  |  |  |  |  |  |
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
  run (compute-on-demand — no fitted artifact is stored here).
- **Engine:** the MBB player-value spine in sdv-py (oracle-gated where
  trained); O/D estimated separately and summed. The fitted coefficient
  vector ships with every publish as
  `mbb_player_value_coefficients.json` (features, intercept + slopes on
  standardized features, moments, λ, fit floor, train seasons,
  sportsdataverse version, artifact sha256).
- **`qualified`:** additive flag, `min >= 300`, set where sd(box_bpm)
  first sits within 10% of its 600–800-minute plateau on the published
  2014–2026 assets (derivation table above; constant
  `QUALIFIED_MIN_MINUTES` in `python/mbb_model_publish/builders.py`,
  recorded in `models/REGISTRY.md`). No row is filtered.
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
- **Resolved (2026-09-01, PR \#25):** the fitted coefficient vector
  ships with every publish as `mbb_player_value_coefficients.json`, and
  the coefficient-importance section above is drawn from it.
- **Resolved (2026-09-01, PR \#25):** the published frame now carries an
  additive `qualified` flag (`min >= 300`, derived from the funnel’s own
  noise curve); low-minute rows are still published, now marked.
