#!/bin/bash
while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done
for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    echo "$i"
    git pull  >> /dev/null
    git config --local user.email "action@github.com"
    git config --local user.name "Github Action"
    Rscript R/espn_mbb_01_pbp_creation.R -s $i -e $i
    Rscript R/espn_mbb_02_team_box_creation.R -s $i -e $i
    Rscript R/espn_mbb_03_player_box_creation.R -s $i -e $i
    git pull  >> /dev/null
    git add mbb/* >> /dev/null
    git pull  >> /dev/null
    git commit -m "MBB Data Update (Start: $i End: $i)" >> /dev/null || echo "No changes to commit"
    git pull  >> /dev/null
    git push  >> /dev/null
done