from glob import glob
import logging
from multiprocessing import Pool

import numpy as np
import pandas as pd
from tqdm import tqdm

def csv_combiner(file_path_str: str) -> pd.DataFrame:
    """ """
    files = glob(file_path_str)

    if len(files) == 0:
        logging.warning("No files found within your specified path.")
        return pd.DataFrame()
    with Pool() as pool:
        df = pd.concat(pool.map(pd.read_csv, tqdm(files, total=len(files))))

    return df


def game_to_season_stats_combiner(year: int):
    """ """
    stats_df = pd.DataFrame()
    stats_df_arr = []
    for i in range(0, 10):
        temp_df = pd.read_csv(
            f"combined_files/game_stats/player/{year}_game_stats_{i}.csv"
        )
        stats_df_arr.append(temp_df)
        del temp_df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    # stats_df = pd.read_csv(
    #     f"combined_files/game_stats/player/{year}_game_stats.csv"
    # )

    batting_df = stats_df.groupby(
        [
            "season",
            "sport_id",
            "team_id",
            "player_id",
            "player_jersey_number",
            "player_full_name",
            "player_positions",
        ],
        as_index=False,
    ).agg(
        {
            "batting_G": "sum",
            "batting_GS": "sum",
            "batting_R": "sum",
            "batting_AB": "sum",
            "batting_H": "sum",
            "batting_2B": "sum",
            "batting_3B": "sum",
            "batting_TB": "sum",
            "batting_HR": "sum",
            "batting_RBI": "sum",
            "batting_BB": "sum",
            "batting_HBP": "sum",
            "batting_SF": "sum",
            "batting_SH": "sum",
            "batting_SO": "sum",
            "batting_OPP_DP": "sum",
            "batting_CS": "sum",
            "batting_PK": "sum",
            "batting_SB": "sum",
            "batting_IBB": "sum",
            "batting_KL": "sum",
        }
    )
    batting_df.to_csv(
        f"combined_files/season_stats/player/{year}_season_batting_stats.csv",
        index=False,
    )

    pitching_df = stats_df.groupby(
        [
            "season",
            "sport_id",
            "team_id",
            "player_id",
            "player_jersey_number",
            "player_full_name",
            "player_positions",
        ],
        as_index=False,
    ).agg(
        {
            "pitching_GP": "sum",
            "pitching_GS": "sum",
            "pitching_IP": "sum",
            "pitching_H": "sum",
            "pitching_R": "sum",
            "pitching_ER": "sum",
            "pitching_BB": "sum",
            "pitching_SO": "sum",
            "pitching_BF": "sum",
            "pitching_2B": "sum",
            "pitching_3B": "sum",
            "pitching_BK": "sum",
            "pitching_HR": "sum",
            "pitching_WP": "sum",
            "pitching_HBP": "sum",
            "pitching_IBB": "sum",
            "pitching_IR": "sum",
            "pitching_IRS": "sum",
            "pitching_SH": "sum",
            "pitching_SF": "sum",
            "pitching_KL": "sum",
            "pitching_TUER": "sum",
            "pitching_PK": "sum",
        }
    )
    pitching_df.to_csv(
        f"combined_files/season_stats/player/{year}_season_pitching_stats.csv",
        index=False,
    )


def combine_raw_pbp():
    """ """
    print("Combining play-by-play data files.")
    pbp_df = csv_combiner("play_by_play_data/raw/*/*.csv")
    pbp_df["game_datetime"] = pd.to_datetime(pbp_df["game_datetime"])

    dates_arr = pbp_df["game_datetime"].to_list()

    dates_arr = list(set(dates_arr))

    pbp_df = pbp_df.sort_values(
        by=["season", "game_datetime", "game_id", "event_num"]
    )
    for datetime_val in dates_arr:
        date_val = str(datetime_val).split(" ")[0]
        temp_df = pbp_df[pbp_df["game_datetime"] == datetime_val]
        if len(temp_df) > 0:
            temp_df.to_csv(
                f"combined_files/pbp_raw/{date_val}_raw_pbp.csv", index=False
            )


def combine_rosters():
    """ """
    print("Combining rosters.")
    roster_df = csv_combiner("player_rosters/*/*.csv")

    season_arr = roster_df["season"].to_list()
    season_arr = list(set(season_arr))
    roster_df = roster_df.sort_values(
        by=["ncaa_division", "school_id", "player_id"]
    )

    for season in season_arr:
        temp_df = roster_df[roster_df["season"] == season]
        temp_df.to_csv(
            f"combined_files/rosters/{season}_rosters.csv", index=False
        )


def combine_game_stats():
    """ """
    number_of_chunks = 10
    print("Combining game stats")
    stats_df = csv_combiner("individual_game_stats/player/*/*.csv")
    season_arr = stats_df["season"].to_list()
    season_arr = list(set(season_arr))
    stats_df = stats_df.sort_values(
        by=["game_datetime", "game_id", "team_id", "player_id"]
    )

    for season in season_arr:
        temp_df = stats_df[stats_df["season"] == season]
        for idx, chunk in enumerate(np.array_split(temp_df, number_of_chunks)):
            chunk.to_csv(
                "combined_files/game_stats/player/" +
                f"{season}_game_stats_{idx}.csv",
                index=False,
            )


def combine_season_stats():
    """ """
    print("Combining batting player season stats.")
    batting_df = csv_combiner("season_stats/player/batting_stats/*/*.csv")

    season_arr = batting_df["season"].to_list()
    season_arr = list(set(season_arr))
    batting_df = batting_df.sort_values(
        by=["ncaa_division", "school_id", "player_id"]
    )

    for season in season_arr:
        temp_df = batting_df[batting_df["season"] == season]
        temp_df.to_csv(
            "combined_files/season_stats/player/"
            + f"{season}_season_batting_stats.csv",
            index=False,
        )

    print("Combining pitching player season stats.")
    pitching_df = csv_combiner("season_stats/player/pitching_stats/*/*.csv")

    season_arr = pitching_df["season"].to_list()
    season_arr = list(set(season_arr))
    pitching_df = pitching_df.sort_values(
        by=["ncaa_division", "school_id", "player_id"]
    )

    for season in season_arr:
        temp_df = pitching_df[pitching_df["season"] == season]
        temp_df.to_csv(
            "combined_files/season_stats/player/"
            + f"{season}_season_pitching_stats.csv",
            index=False,
        )

    print("Combining fielding player season stats.")
    fielding_df = csv_combiner("season_stats/player/fielding_stats/*/*.csv")

    season_arr = fielding_df["season"].to_list()
    season_arr = list(set(season_arr))
    fielding_df = fielding_df.sort_values(
        by=["ncaa_division", "school_id", "player_id"]
    )

    for season in season_arr:
        temp_df = fielding_df[fielding_df["season"] == season]
        temp_df.to_csv(
            "combined_files/season_stats/player/"
            + f"{season}_season_fielding_stats.csv",
            index=False,
        )


def main():
    """ """
    combine_rosters()
    combine_season_stats()
    combine_game_stats()
    game_to_season_stats_combiner(2025)
    combine_raw_pbp()


if __name__ == "__main__":
    main()
