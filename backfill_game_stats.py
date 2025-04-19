from ncaa_stats_py.baseball import (
    get_baseball_player_game_batting_stats,
    get_baseball_player_game_pitching_stats,
    get_baseball_player_season_fielding_stats,
)
from tqdm import tqdm
import pandas as pd
from combine import csv_combiner


def backfill_game_stats(season: int):
    """ """
    roster_df = pd.read_csv(
        f"combined_files/rosters/{season}_rosters.csv"
    )
    roster_df = roster_df[roster_df["player_G"] > 0]
    player_id_arr = roster_df["player_id"].to_list()

    for player_id in tqdm(player_id_arr):
        b_df = get_baseball_player_game_batting_stats(
            player_id=player_id,
            season=season
        )
        b_df.to_csv(
            "game_stats/player/batting_stats/" +
            f"{season}_{player_id}_batting_game_stats.csv",
            index=False
        )

        del b_df

        p_df = get_baseball_player_game_pitching_stats(
            player_id=player_id,
            season=season
        )
        p_df.to_csv(
            "game_stats/player/pitching_stats/" +
            f"{season}_{player_id}_pitching_game_stats.csv",
            index=False
        )

        del p_df

        f_df = get_baseball_player_season_fielding_stats(
            player_id=player_id,
            season=season
        )
        f_df.to_csv(
            "game_stats/player/fielding_stats/" +
            f"{season}_{player_id}_fielding_game_stats.csv",
            index=False
        )

        del f_df


def main():
    """ """
    year = 2024
    backfill_game_stats(year)


if __name__ == "__main__":
    main()
