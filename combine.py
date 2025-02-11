from glob import glob
import logging
from multiprocessing import Pool

import pandas as pd
from tqdm import tqdm


def csv_combiner(file_path_str: str) -> pd.DataFrame:
    """ """
    files = glob(file_path_str)

    if len(files) == 0:
        logging.warning(
            "No files found within your specified path."
        )
        return pd.DataFrame()
    with Pool() as pool:
        df = pd.concat(pool.map(pd.read_csv, tqdm(files, total=len(files))))

    return df


if __name__ == "__main__":
    print(csv_combiner("./individual_game_stats/player/2025/*.csv"))
