import logging
from os import mkdir
from random import shuffle

import pandas as pd
from ncaa_stats_py.baseball import (
    get_baseball_team_roster,
    get_baseball_teams
)
from tqdm import tqdm

from combine import csv_combiner


def download_rosters(teams_df: pd.DataFrame, year: int):
    """ """
    teams_df = teams_df[teams_df["season"] == year]
    try:
        mkdir(f"player_rosters/{year}")
    except FileExistsError:
        logging.info(
            f"`player_rosters/{year}` already exists"
        )
    except Exception as e:
        raise e

    team_ids_arr = teams_df["team_id"].to_list()
    shuffle(team_ids_arr)

    for team_id in tqdm(team_ids_arr):
        df = get_baseball_team_roster(team_id=team_id)
        if len(df) > 0:
            df.to_csv(
                f"player_rosters/{year}/{team_id}_roster.csv",
                index=False
            )


def combine_rosters(year: int):
    roster_df = csv_combiner(f"player_rosters/{year}/*.csv")
    roster_df = roster_df.sort_values(
        by=["ncaa_division", "school_id", "player_id"]
    )
    roster_df.to_csv(
        f"player_rosters/{year}_rosters.csv",
        index=False
    )


def main():
    """ """
    year = 2025
    for i in range(year, 2014, -1):
        for division_str in ["I", "II", "III"]:
            print(f"\nParsing {i} D{division_str} rosters")

            df = get_baseball_teams(season=i, level=division_str)
            download_rosters(teams_df=df, year=i)
            combine_rosters(year=i)


if __name__ == "__main__":
    main()
