import logging
from os import mkdir
from random import shuffle
import time

import pandas as pd
from ncaa_stats_py.baseball import (
    get_baseball_team_roster,
    load_baseball_teams
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
        f"combined_files/rosters/{year}_rosters.csv",
        index=False
    )


def main():
    """ """
    year = 2025
    df = load_baseball_teams()
    for i in range(year, 2013, -1):
        print(f"\nParsing {i} rosters")

        download_rosters(teams_df=df, year=i)
        # combine_rosters(year=i)


if __name__ == "__main__":
    main()
