import logging
from datetime import date, datetime, timedelta
from os import mkdir
from os.path import exists
import time

import pandas as pd
from ncaa_stats_py.baseball import (
    get_baseball_day_schedule,
    get_baseball_game_player_stats,
    get_raw_baseball_game_pbp,
)
from tqdm import tqdm


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

    # try:
    #     mkdir(f"individual_game_stats/team/{season}")
    # except FileExistsError:
    #     logging.info(
    #         f"`individual_game_stats/team/{season}` already exists"
    #     )
    # except Exception as e:
    #     raise e

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
        if len(schedule_df) == 0:
            continue

        schedule_df = schedule_df[
            (schedule_df["home_runs_scored"] > 0) |
            (schedule_df["away_runs_scored"] > 0)
        ]

        game_ids_arr = schedule_df["game_id"].to_numpy()
        for game_id in tqdm(game_ids_arr):
            if (
                exists(
                    f"individual_game_stats/player/{season}/" +
                    f"{game_id}_player_game_stats.csv"
                ) and exists(
                    f"play_by_play_data/raw/{season}/" +
                    f"{game_id}_raw_pbp.csv"
                )
            ):
                continue

            try:
                player_stats_df = get_baseball_game_player_stats(game_id)
            except Exception as e:
                logging.warning(
                    f"Unhandled exception: `{e}`, game ID: {game_id}"
                )
                time.sleep(5)
                continue

            if len(player_stats_df) > 0:
                player_stats_df.to_csv(
                    f"individual_game_stats/player/{season}/" +
                    f"{game_id}_player_game_stats.csv",
                    index=False
                )

            try:
                raw_pbp_df = get_raw_baseball_game_pbp(game_id)
            except Exception as e:
                logging.warning(
                    f"Unhandled exception: `{e}`, game ID: {game_id}"
                )
                time.sleep(5)
                continue

            if len(raw_pbp_df) > 0:
                raw_pbp_df.to_csv(
                    f"play_by_play_data/raw/{season}/" +
                    f"{game_id}_raw_pbp.csv",
                    index=False
                )


def main():
    """ """

    now_datetime = datetime.now() - timedelta(days=1)
    later_datetime = datetime.now() - timedelta(days=3)

    # now_datetime = datetime(
    #     year=2025,
    #     month=3,
    #     day=1
    # ) - timedelta(days=1)
    # later_datetime = datetime(
    #     year=2025,
    #     month=2,
    #     day=1
    # ) - timedelta(days=1)

    date_list = pd.date_range(
        start=later_datetime,
        end=now_datetime
    )

    for day in date_list:
        get_day_game_stats(date_obj=day)


if __name__ == "__main__":
    main()
