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
        try:
            df = get_baseball_player_season_batting_stats(team_id=team_id)
            df = df.sort_values(
                by=["season", "school_id", "player_id"]
            )
        except Exception as e:
            df = pd.DataFrame()
            logging.warning(
                f"Could not get batting stats for team ID {team_id}. " +
                f"Full exception: `{e}`"
            )
        if len(df) > 0:
            df.to_csv(
                "season_stats/player/batting_stats/" +
                f"{year}/{team_id}_season_batting_stats.csv",
                index=False
            )

        try:
            df = get_baseball_player_season_pitching_stats(team_id=team_id)
            df = df.sort_values(
                by=["season", "school_id", "player_id"]
            )
        except Exception as e:
            df = pd.DataFrame()
            logging.warning(
                f"Could not get pitching stats for team ID {team_id}. " +
                f"Full exception: `{e}`"
            )

        if len(df) > 0:
            df.to_csv(
                "season_stats/player/pitching_stats/" +
                f"{year}/{team_id}_season_pitching_stats.csv",
                index=False
            )

        if year >= 2017:
            try:
                df = get_baseball_player_season_fielding_stats(team_id=team_id)
                df = df.sort_values(
                    by=["season", "school_id", "player_id"]
                )
            except Exception as e:
                df = pd.DataFrame()
                logging.warning(
                    f"Could not get fielding stats for team ID {team_id}. " +
                    f"Full exception: `{e}`"
                )

            if len(df) > 0:
                df.to_csv(
                    "season_stats/player/fielding_stats/" +
                    f"{year}/{team_id}_season_fielding_stats.csv",
                    index=False
                )


def main():
    """ """
    year = 2022
    for i in range(year, 2013, -1):
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
