import logging
from datetime import date, datetime, timedelta
from os import mkdir

# import pandas as pd
from ncaa_stats_py.baseball import (
    get_baseball_day_schedule,
    get_baseball_game_player_stats,
    get_baseball_game_team_stats,
    get_raw_baseball_game_pbp,
)
from tqdm import tqdm

from combine import csv_combiner


def get_day_game_stats(date_obj: date):
    """ """
    season = date_obj.year
    try:
        mkdir(f"individual_game_stats/player/{season}")
    except FileExistsError:
        logging.info(
            f"`individual_game_stats/player/{season}` already exists"
        )
    except Exception as e:
        raise e

    try:
        mkdir(f"individual_game_stats/team/{season}")
    except FileExistsError:
        logging.info(
            f"`individual_game_stats/team/{season}` already exists"
        )
    except Exception as e:
        raise e

    try:
        mkdir(f"play_by_play_data/raw/{season}")
    except FileExistsError:
        logging.info(
            f"`play_by_play_data/raw/{season}` already exists"
        )
    except Exception as e:
        raise e

    for level in ["I", "II", "III"]:

        print(
            f"Getting game data for {date_obj}, at the D{level} level."
        )
        schedule_df = get_baseball_day_schedule(date_obj, level=level)
        schedule_df = schedule_df[
            (schedule_df["home_runs_scored"] > 0) |
            (schedule_df["away_runs_scored"] > 0)
        ]

        if len(schedule_df) == 0:
            continue

        game_ids_arr = schedule_df["game_id"].to_numpy()
        for game_id in tqdm(game_ids_arr):
            try:
                player_stats_df = get_baseball_game_player_stats(game_id)
            except Exception as e:
                logging.warning(
                    f"Unhandled exception: `{e}`, game ID: {game_id}"
                )
                continue

            if len(player_stats_df) > 0:
                player_stats_df.to_csv(
                    f"individual_game_stats/player/{season}/" +
                    f"{game_id}_player_game_stats.csv",
                    index=False
                )

            try:
                team_stats_df = get_baseball_game_team_stats(game_id)
            except Exception as e:
                logging.warning(
                    f"Unhandled exception: `{e}`, game ID: {game_id}"
                )
                continue
            if len(team_stats_df) > 0:

                team_stats_df.to_csv(
                    f"individual_game_stats/team/{season}/" +
                    f"{game_id}_team_game_stats.csv",
                    index=False
                )

            try:
                raw_pbp_df = get_raw_baseball_game_pbp(game_id)
            except Exception as e:
                logging.warning(
                    f"Unhandled exception: `{e}`, game ID: {game_id}"
                )
                continue

            if len(raw_pbp_df) > 0:
                raw_pbp_df.to_csv(
                    f"play_by_play_data/raw/{season}/" +
                    f"{game_id}_raw_pbp.csv",
                    index=False
                )

    player_file_path = f"individual_game_stats/player/{season}/*.csv"
    comb_player_df = csv_combiner(player_file_path)
    comb_player_df.to_csv(
        f"individual_game_stats/player/{season}_player_game_stats.csv",
        index=False
    )

    team_file_path = f"individual_game_stats/team/{season}/*.csv"
    comb_player_df = csv_combiner(team_file_path)
    comb_player_df.to_csv(
        f"individual_game_stats/team/{season}_team_game_stats.csv",
        index=False
    )

    # raw_pbp_file_path = f"individual_game_stats/player/{season}/*.csv"


def main():
    """ """
    now = datetime.now() - timedelta(days=1)
    year = now.year
    month = now.month
    for day in range(now.day, 1, -1):
        date_obj = date(
            year=year,
            month=month,
            day=day
        )
        get_day_game_stats(date_obj=date_obj)


if __name__ == "__main__":
    main()
