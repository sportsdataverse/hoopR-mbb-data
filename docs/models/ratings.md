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
rating surface, identified team-level results, and the absolute level
gate that guards the scale a rank check cannot see.

## Training data

<div id="nvaxoygcvt" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#nvaxoygcvt table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#nvaxoygcvt thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#nvaxoygcvt p { margin: 0; padding: 0; }
 #nvaxoygcvt .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #nvaxoygcvt .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #nvaxoygcvt .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #nvaxoygcvt .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #nvaxoygcvt .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #nvaxoygcvt .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nvaxoygcvt .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #nvaxoygcvt .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #nvaxoygcvt .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #nvaxoygcvt .gt_column_spanner_outer:first-child { padding-left: 0; }
 #nvaxoygcvt .gt_column_spanner_outer:last-child { padding-right: 0; }
 #nvaxoygcvt .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #nvaxoygcvt .gt_spanner_row { border-bottom-style: hidden; }
 #nvaxoygcvt .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #nvaxoygcvt .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #nvaxoygcvt .gt_from_md> :first-child { margin-top: 0; }
 #nvaxoygcvt .gt_from_md> :last-child { margin-bottom: 0; }
 #nvaxoygcvt .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #nvaxoygcvt .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #nvaxoygcvt .gt_indent_1 { text-indent: 5px; }
 #nvaxoygcvt .gt_indent_2 { text-indent: calc(5px * 2); }
 #nvaxoygcvt .gt_indent_3 { text-indent: calc(5px * 3); }
 #nvaxoygcvt .gt_indent_4 { text-indent: calc(5px * 4); }
 #nvaxoygcvt .gt_indent_5 { text-indent: calc(5px * 5); }
 #nvaxoygcvt .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #nvaxoygcvt .gt_row_group_first td { border-top-width: 2px; }
 #nvaxoygcvt .gt_row_group_first th { border-top-width: 2px; }
 #nvaxoygcvt .gt_striped { color: #333333; background-color: #F4F4F4; }
 #nvaxoygcvt .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nvaxoygcvt .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #nvaxoygcvt .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #nvaxoygcvt .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nvaxoygcvt .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #nvaxoygcvt .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #nvaxoygcvt .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #nvaxoygcvt .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nvaxoygcvt .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #nvaxoygcvt .gt_left { text-align: left; }
 #nvaxoygcvt .gt_center { text-align: center; }
 #nvaxoygcvt .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #nvaxoygcvt .gt_font_normal { font-weight: normal; }
 #nvaxoygcvt .gt_font_bold { font-weight: bold; }
 #nvaxoygcvt .gt_font_italic { font-style: italic; }
 #nvaxoygcvt .gt_super { font-size: 65%; }
 #nvaxoygcvt .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nvaxoygcvt .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #nvaxoygcvt .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nvaxoygcvt .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #nvaxoygcvt .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #nvaxoygcvt .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Published mbb_ratings assets, by season |  |  |  |  |  |
|----|----|----|----|----|----|
| the full frame carries every opponent ever seen (few-game non-D1 teams pull its mean far negative); the core -- teams with 10+ games -- sits near zero |  |  |  |  |  |
| season | teams | teams_10plus_games | team_games | mean_adj_em_all | mean_adj_em_core |
| 2006 | 526 | 334 | 9,846 | −14.01 | 0.14 |
| 2007 | 539 | 336 | 10,488 | −14.17 | 0.33 |
| 2008 | 532 | 342 | 10,810 | −12.91 | 0.16 |
| 2009 | 546 | 345 | 11,278 | −13.58 | 0.29 |
| 2010 | 608 | 347 | 11,496 | −16.43 | 0.54 |
| 2011 | 604 | 346 | 11,508 | <na> | <na> |
| 2012 | 600 | 345 | 11,518 | −16.43 | 0.63 |
| 2013 | 581 | 347 | 11,598 | −16.71 | 0.67 |
| 2014 | 626 | 351 | 11,850 | −18.11 | 1.00 |
| 2015 | 637 | 351 | 11,854 | −18.22 | 0.98 |
| 2016 | 639 | 351 | 11,762 | −17.39 | 0.93 |
| 2017 | 641 | 350 | 11,844 | −18.69 | 1.09 |
| 2018 | 658 | 351 | 12,004 | −19.62 | 1.29 |
| 2019 | 652 | 353 | 12,094 | −20.22 | 1.28 |
| 2020 | 658 | 353 | 11,514 | −21.27 | 1.88 |
| 2021 | 493 | 348 | 8,566 | −11.46 | 0.09 |
| 2022 | 679 | 360 | 11,930 | −22.18 | 1.24 |
| 2023 | 706 | 363 | 12,440 | −23.36 | 1.68 |
| 2024 | 717 | 362 | 12,480 | −23.97 | 1.70 |
| 2025 | 700 | 365 | 12,572 | −24.65 | 1.65 |
| 2026 | 727 | 366 | 12,598 | −25.75 | 1.72 |

&#10;</div>

Inputs are the published season pbp/box assets of this repository — the
ratings sit downstream of the same daily pipeline that publishes the
data they are computed from, which is what keeps them reproducible. A
season row whose means are blank is a published asset the fixed point
never converged on (every rating NaN); the level gate below now refuses
such a season at publish time.

## Exploratory data analysis

<img src="ratings_files/figure-commonmark/cell-4-output-1.png"
width="420" height="300"
alt="The rating surface: adjusted offense vs adjusted defense (defense lower = better), latest season." />

<img src="ratings_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300"
alt="Adjustment at work: adjusted net vs raw net. Off-diagonal teams are schedule effects." />

<div id="hxvcaqkrit" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#hxvcaqkrit table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#hxvcaqkrit thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#hxvcaqkrit p { margin: 0; padding: 0; }
 #hxvcaqkrit .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #hxvcaqkrit .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #hxvcaqkrit .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #hxvcaqkrit .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #hxvcaqkrit .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #hxvcaqkrit .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #hxvcaqkrit .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #hxvcaqkrit .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #hxvcaqkrit .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #hxvcaqkrit .gt_column_spanner_outer:first-child { padding-left: 0; }
 #hxvcaqkrit .gt_column_spanner_outer:last-child { padding-right: 0; }
 #hxvcaqkrit .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #hxvcaqkrit .gt_spanner_row { border-bottom-style: hidden; }
 #hxvcaqkrit .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #hxvcaqkrit .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #hxvcaqkrit .gt_from_md> :first-child { margin-top: 0; }
 #hxvcaqkrit .gt_from_md> :last-child { margin-bottom: 0; }
 #hxvcaqkrit .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #hxvcaqkrit .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #hxvcaqkrit .gt_indent_1 { text-indent: 5px; }
 #hxvcaqkrit .gt_indent_2 { text-indent: calc(5px * 2); }
 #hxvcaqkrit .gt_indent_3 { text-indent: calc(5px * 3); }
 #hxvcaqkrit .gt_indent_4 { text-indent: calc(5px * 4); }
 #hxvcaqkrit .gt_indent_5 { text-indent: calc(5px * 5); }
 #hxvcaqkrit .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #hxvcaqkrit .gt_row_group_first td { border-top-width: 2px; }
 #hxvcaqkrit .gt_row_group_first th { border-top-width: 2px; }
 #hxvcaqkrit .gt_striped { color: #333333; background-color: #F4F4F4; }
 #hxvcaqkrit .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #hxvcaqkrit .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #hxvcaqkrit .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #hxvcaqkrit .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #hxvcaqkrit .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #hxvcaqkrit .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #hxvcaqkrit .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #hxvcaqkrit .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #hxvcaqkrit .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #hxvcaqkrit .gt_left { text-align: left; }
 #hxvcaqkrit .gt_center { text-align: center; }
 #hxvcaqkrit .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #hxvcaqkrit .gt_font_normal { font-weight: normal; }
 #hxvcaqkrit .gt_font_bold { font-weight: bold; }
 #hxvcaqkrit .gt_font_italic { font-style: italic; }
 #hxvcaqkrit .gt_super { font-size: 65%; }
 #hxvcaqkrit .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #hxvcaqkrit .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #hxvcaqkrit .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #hxvcaqkrit .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #hxvcaqkrit .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #hxvcaqkrit .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Internal consistency — 2026                      |        |
|--------------------------------------------------|--------|
| check                                            | value  |
| mean adj_em, teams with 10+ games (should be ~0) | 1.7156 |
| corr(adj_em, raw margin)                         | 0.9544 |
| corr(adj_em, adj_em_z) (should be ~1)            | 1.0000 |

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

<div id="yghdrzzben" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#yghdrzzben table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#yghdrzzben thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#yghdrzzben p { margin: 0; padding: 0; }
 #yghdrzzben .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #yghdrzzben .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #yghdrzzben .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #yghdrzzben .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #yghdrzzben .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #yghdrzzben .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #yghdrzzben .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #yghdrzzben .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #yghdrzzben .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #yghdrzzben .gt_column_spanner_outer:first-child { padding-left: 0; }
 #yghdrzzben .gt_column_spanner_outer:last-child { padding-right: 0; }
 #yghdrzzben .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #yghdrzzben .gt_spanner_row { border-bottom-style: hidden; }
 #yghdrzzben .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #yghdrzzben .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #yghdrzzben .gt_from_md> :first-child { margin-top: 0; }
 #yghdrzzben .gt_from_md> :last-child { margin-bottom: 0; }
 #yghdrzzben .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #yghdrzzben .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #yghdrzzben .gt_indent_1 { text-indent: 5px; }
 #yghdrzzben .gt_indent_2 { text-indent: calc(5px * 2); }
 #yghdrzzben .gt_indent_3 { text-indent: calc(5px * 3); }
 #yghdrzzben .gt_indent_4 { text-indent: calc(5px * 4); }
 #yghdrzzben .gt_indent_5 { text-indent: calc(5px * 5); }
 #yghdrzzben .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #yghdrzzben .gt_row_group_first td { border-top-width: 2px; }
 #yghdrzzben .gt_row_group_first th { border-top-width: 2px; }
 #yghdrzzben .gt_striped { color: #333333; background-color: #F4F4F4; }
 #yghdrzzben .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #yghdrzzben .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #yghdrzzben .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #yghdrzzben .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #yghdrzzben .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #yghdrzzben .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #yghdrzzben .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #yghdrzzben .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #yghdrzzben .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #yghdrzzben .gt_left { text-align: left; }
 #yghdrzzben .gt_center { text-align: center; }
 #yghdrzzben .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #yghdrzzben .gt_font_normal { font-weight: normal; }
 #yghdrzzben .gt_font_bold { font-weight: bold; }
 #yghdrzzben .gt_font_italic { font-style: italic; }
 #yghdrzzben .gt_super { font-size: 65%; }
 #yghdrzzben .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #yghdrzzben .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #yghdrzzben .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #yghdrzzben .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #yghdrzzben .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #yghdrzzben .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Program stickiness — adj_em season S vs S+1, same team |  |  |
|----|----|----|
| published-asset check; programs are persistent, rosters are not — the gap is roster turnover |  |  |
| season | yoy_pearson | teams |
| 2006 | 0.875 | 434 |
| 2007 | 0.869 | 445 |
| 2008 | 0.863 | 443 |
| 2009 | 0.880 | 465 |
| 2010 | <na> | 489 |
| 2011 | <na> | 482 |
| 2012 | 0.862 | 482 |
| 2013 | 0.859 | 484 |
| 2014 | 0.873 | 500 |
| 2015 | 0.857 | 521 |
| 2016 | 0.859 | 513 |
| 2017 | 0.875 | 517 |
| 2018 | 0.881 | 521 |
| 2019 | 0.888 | 529 |
| 2020 | 0.896 | 410 |
| 2021 | 0.883 | 423 |
| 2022 | 0.884 | 556 |
| 2023 | 0.884 | 578 |
| 2024 | 0.890 | 572 |
| 2025 | 0.897 | 577 |

&#10;</div>

## Level gate — the scale check a rank gate cannot do

Spearman is invariant to any monotone rescale: multiply every rating by
100, divide by the games played, or flip the sign convention and the
rank correlation against KenPom or Torvik does not move. That is how a
ratings scale bug ships past a rank-only gate (it happened in this
ecosystem’s CFB ratings). The publish path of this repository therefore
carries an **absolute level gate** beside the engine’s rank gates: over
the core — teams with at least `MIN_GAMES_GATED` games — the season’s
mean `adj_o`, `adj_d`, `adj_em` and `adj_tempo` and the spread of
`adj_em` must sit inside bands set from the observed published seasons
and in-season snapshots, with no non-finite value; it applies once
`MIN_GATED_TEAMS` teams qualify and logs, rather than pretends, before
that. The table is the gate re-run at render time on the assets
consumers download.

<div id="zpjqjbvxna" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#zpjqjbvxna table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#zpjqjbvxna thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#zpjqjbvxna p { margin: 0; padding: 0; }
 #zpjqjbvxna .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #zpjqjbvxna .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #zpjqjbvxna .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #zpjqjbvxna .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #zpjqjbvxna .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zpjqjbvxna .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zpjqjbvxna .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zpjqjbvxna .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #zpjqjbvxna .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #zpjqjbvxna .gt_column_spanner_outer:first-child { padding-left: 0; }
 #zpjqjbvxna .gt_column_spanner_outer:last-child { padding-right: 0; }
 #zpjqjbvxna .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #zpjqjbvxna .gt_spanner_row { border-bottom-style: hidden; }
 #zpjqjbvxna .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #zpjqjbvxna .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #zpjqjbvxna .gt_from_md> :first-child { margin-top: 0; }
 #zpjqjbvxna .gt_from_md> :last-child { margin-bottom: 0; }
 #zpjqjbvxna .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #zpjqjbvxna .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #zpjqjbvxna .gt_indent_1 { text-indent: 5px; }
 #zpjqjbvxna .gt_indent_2 { text-indent: calc(5px * 2); }
 #zpjqjbvxna .gt_indent_3 { text-indent: calc(5px * 3); }
 #zpjqjbvxna .gt_indent_4 { text-indent: calc(5px * 4); }
 #zpjqjbvxna .gt_indent_5 { text-indent: calc(5px * 5); }
 #zpjqjbvxna .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #zpjqjbvxna .gt_row_group_first td { border-top-width: 2px; }
 #zpjqjbvxna .gt_row_group_first th { border-top-width: 2px; }
 #zpjqjbvxna .gt_striped { color: #333333; background-color: #F4F4F4; }
 #zpjqjbvxna .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zpjqjbvxna .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zpjqjbvxna .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #zpjqjbvxna .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zpjqjbvxna .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zpjqjbvxna .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #zpjqjbvxna .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #zpjqjbvxna .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zpjqjbvxna .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zpjqjbvxna .gt_left { text-align: left; }
 #zpjqjbvxna .gt_center { text-align: center; }
 #zpjqjbvxna .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #zpjqjbvxna .gt_font_normal { font-weight: normal; }
 #zpjqjbvxna .gt_font_bold { font-weight: bold; }
 #zpjqjbvxna .gt_font_italic { font-style: italic; }
 #zpjqjbvxna .gt_super { font-size: 65%; }
 #zpjqjbvxna .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zpjqjbvxna .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #zpjqjbvxna .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zpjqjbvxna .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zpjqjbvxna .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #zpjqjbvxna .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Level gate re-run on the published assets (teams with 10+ games; applies at 150+ such teams) |  |  |  |  |  |  |  |  |
|----|----|----|----|----|----|----|----|----|
| bands: mean adj_o in \[95, 118\], mean adj_d in \[95, 118\], mean adj_em in \[-8, 8\], mean adj_tempo in \[60, 76\], sd adj_em in \[10, 22\] |  |  |  |  |  |  |  |  |
| season | core_teams | non_finite | mean_adj_o | mean_adj_d | mean_adj_em | mean_adj_tempo | sd_adj_em | verdict |
| 2006 | 334 | 0 | 102.30 | 102.16 | 0.14 | 67.33 | 13.51 | pass |
| 2007 | 336 | 0 | 102.79 | 102.46 | 0.33 | 67.14 | 14.57 | pass |
| 2008 | 342 | 0 | 102.45 | 102.29 | 0.16 | 67.38 | 14.53 | pass |
| 2009 | 345 | 0 | 102.12 | 101.83 | 0.29 | 66.78 | 14.05 | pass |
| 2010 | 347 | 0 | 102.51 | 101.97 | 0.54 | 67.18 | 14.76 | pass |
| 2011 | 346 | 346 | <na> | <na> | <na> | <na> | <na> | REFUSED: non-finite ratings |
| 2012 | 345 | 0 | 102.38 | 101.75 | 0.63 | 66.19 | 13.84 | pass |
| 2013 | 347 | 0 | 101.90 | 101.23 | 0.67 | 65.99 | 14.07 | pass |
| 2014 | 351 | 0 | 106.16 | 105.16 | 1.00 | 66.66 | 13.90 | pass |
| 2015 | 351 | 0 | 103.58 | 102.60 | 0.98 | 65.08 | 14.10 | pass |
| 2016 | 351 | 0 | 105.17 | 104.23 | 0.93 | 69.19 | 14.07 | pass |
| 2017 | 350 | 0 | 105.11 | 104.02 | 1.09 | 69.59 | 14.22 | pass |
| 2018 | 351 | 0 | 105.69 | 104.40 | 1.29 | 69.61 | 13.79 | pass |
| 2019 | 353 | 0 | 104.69 | 103.42 | 1.28 | 69.26 | 13.83 | pass |
| 2020 | 353 | 0 | 102.58 | 100.71 | 1.88 | 69.26 | 13.51 | pass |
| 2021 | 348 | 0 | 102.28 | 102.19 | 0.09 | 69.50 | 15.41 | pass |
| 2022 | 360 | 0 | 103.37 | 102.13 | 1.24 | 68.40 | 14.28 | pass |
| 2023 | 363 | 0 | 104.86 | 103.18 | 1.68 | 68.18 | 13.17 | pass |
| 2024 | 362 | 0 | 106.87 | 105.17 | 1.70 | 68.65 | 14.06 | pass |
| 2025 | 365 | 0 | 107.95 | 106.29 | 1.65 | 68.17 | 15.72 | pass |
| 2026 | 366 | 0 | 110.24 | 108.52 | 1.72 | 68.50 | 16.45 | pass |

&#10;</div>

The bands were set on 2026-09-01 from the published 2006–2026 assets
plus in-season engine snapshots (2024, 2025 and 2026 from Dec 10 to
season end): core teams 334–366 at season end and 153+ from about Dec
10; mean adj_o 101.9–111.1; mean adj_d 100.7–108.5; mean adj_em 0.09–6.0
(the high end is mid-December, when the core is small and unbalanced; ≤
1.9 at season end); sd adj_em 13.2–16.7; mean adj_tempo 65.1–70.0. Each
band is the observed range padded so a real season never trips it while
a unit or scale error does — per-game instead of per-100 divides every
level by ~1.5, a sign flip mirrors adj_em, and an un-converged fixed
point (the published 2011 asset, every team NaN) fails the finiteness
check. A season the table marks REFUSED would not be republished by the
current builder until the engine input is repaired.

## Results

<div id="rwueahkbej" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#rwueahkbej table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#rwueahkbej thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#rwueahkbej p { margin: 0; padding: 0; }
 #rwueahkbej .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #rwueahkbej .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #rwueahkbej .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #rwueahkbej .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #rwueahkbej .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #rwueahkbej .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #rwueahkbej .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #rwueahkbej .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #rwueahkbej .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #rwueahkbej .gt_column_spanner_outer:first-child { padding-left: 0; }
 #rwueahkbej .gt_column_spanner_outer:last-child { padding-right: 0; }
 #rwueahkbej .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #rwueahkbej .gt_spanner_row { border-bottom-style: hidden; }
 #rwueahkbej .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #rwueahkbej .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #rwueahkbej .gt_from_md> :first-child { margin-top: 0; }
 #rwueahkbej .gt_from_md> :last-child { margin-bottom: 0; }
 #rwueahkbej .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #rwueahkbej .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #rwueahkbej .gt_indent_1 { text-indent: 5px; }
 #rwueahkbej .gt_indent_2 { text-indent: calc(5px * 2); }
 #rwueahkbej .gt_indent_3 { text-indent: calc(5px * 3); }
 #rwueahkbej .gt_indent_4 { text-indent: calc(5px * 4); }
 #rwueahkbej .gt_indent_5 { text-indent: calc(5px * 5); }
 #rwueahkbej .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #rwueahkbej .gt_row_group_first td { border-top-width: 2px; }
 #rwueahkbej .gt_row_group_first th { border-top-width: 2px; }
 #rwueahkbej .gt_striped { color: #333333; background-color: #F4F4F4; }
 #rwueahkbej .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #rwueahkbej .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #rwueahkbej .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #rwueahkbej .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #rwueahkbej .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #rwueahkbej .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #rwueahkbej .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #rwueahkbej .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #rwueahkbej .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #rwueahkbej .gt_left { text-align: left; }
 #rwueahkbej .gt_center { text-align: center; }
 #rwueahkbej .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #rwueahkbej .gt_font_normal { font-weight: normal; }
 #rwueahkbej .gt_font_bold { font-weight: bold; }
 #rwueahkbej .gt_font_italic { font-style: italic; }
 #rwueahkbej .gt_super { font-size: 65%; }
 #rwueahkbej .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #rwueahkbej .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #rwueahkbej .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #rwueahkbej .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #rwueahkbej .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #rwueahkbej .gt_asterisk { font-size: 100%; vertical-align: 0; }
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
  adjustment (em-scale fixed point); engine training + oracle (rank)
  gates live in sdv-py.
- **Level gate:**
  `python/mbb_model_publish/builders.py::assert_ratings_level` (bands,
  floor and observations recorded in `models/REGISTRY.md`); its
  per-season record is written into the `mbb_ratings_card.json` sidecar.
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
- **Resolved (2026-09-01, PR \#25):** the scale-blindness of the
  Spearman-style checks is closed in this repository by the absolute
  level gate above (`assert_ratings_level`), run at publish beside
  sdv-py’s rank gates and re-run on the published assets in this
  document.
- **Known issue:** the published `mbb_ratings_2011.parquet` is entirely
  NaN (the fixed point did not converge on that season’s inputs); the
  level gate refuses it, so the repair is upstream in the engine/inputs,
  then a republish.
