import logging
from os import mkdir
from random import shuffle

import pandas as pd
from ncaa_stats_py.baseball import (
    get_baseball_player_season_fielding_stats,
    get_baseball_player_season_batting_stats,
    get_baseball_player_season_pitching_stats,
    get_baseball_teams
)
from tqdm import tqdm


def get_season_stats(teams_df: pd.DataFrame, year: int):
    """ """
    teams_df = teams_df[teams_df["season"] == year]
    try:
        mkdir(f"season_stats/player/batting_stats/{year}")
    except FileExistsError:
        logging.info(
            f"`season_stats/player/batting_stats/{year}` already exists"
        )
    except Exception as e:
        raise e

    try:
        mkdir(f"season_stats/player/pitching_stats/{year}")
    except FileExistsError:
        logging.info(
            f"`season_stats/player/pitching_stats/{year}` already exists"
        )
    except Exception as e:
        raise e

    try:
        mkdir(f"season_stats/player/fielding_stats/{year}")
    except FileExistsError:
        logging.info(
            f"`season_stats/player/fielding_stats/{year}` already exists"
        )
    except Exception as e:
        raise e

    team_ids_arr = teams_df["team_id"].to_list()
    shuffle(team_ids_arr)

    for team_id in tqdm(team_ids_arr):
        df = get_baseball_player_season_batting_stats(team_id=team_id)
        if len(df) > 0:
            df.to_csv(
                "season_stats/player/batting_stats/" +
                f"{year}/{team_id}_season_batting_stats.csv",
                index=False
            )

        df = get_baseball_player_season_pitching_stats(team_id=team_id)
        if len(df) > 0:
            df.to_csv(
                "season_stats/player/pitching_stats/" +
                f"{year}/{team_id}_season_pitching_stats.csv",
                index=False
            )

        if year >= 2017:
            df = get_baseball_player_season_fielding_stats(team_id=team_id)
            if len(df) > 0:
                df.to_csv(
                    "season_stats/player/fielding_stats/" +
                    f"{year}/{team_id}_season_fielding_stats.csv",
                    index=False
                )


def main():
    """ """
    year = 2024
    for i in range(year, 2014, -1):
        print(
            f"Parsing {i} season stats"
        )
        for div_str in [
            "I",
            "II",
            "III"
        ]:
            print(f"Parsing D{div_str} stats")
            df = get_baseball_teams(season=i, level=div_str)
            get_season_stats(teams_df=df, year=i)


if __name__ == "__main__":
    main()
