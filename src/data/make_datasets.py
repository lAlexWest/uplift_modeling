import os
from dotenv import find_dotenv, load_dotenv

import numpy as np
import pandas as pd
import sqlite3

from sklift import datasets
from pathlib import Path


def make_datasets_megafon():
    '''Fetches raw megafon data'''    

    # SQLite connection and Project directory
    project_dir = Path(__file__).resolve().parents[2]
    conn = sqlite3.connect(os.environ['DATABASE_URL'])

    # Load all datasets and tide them and store to SQLlite
    megafon_data = datasets.fetch_megafon()
    df = pd.DataFrame(megafon_data['data'])

    # Concat to wide df
    df = pd.concat([df, megafon_data['target'].to_frame(), megafon_data['treatment']], axis=1)

    # Store raw data
    chunksize = int(np.ceil(32766 / len(df.columns)))
    num_rows = df.to_sql(
        'megafon_raw', conn, if_exists='replace', index=False, 
        chunksize=chunksize, method='multi'
        )
    Path(f'{project_dir}/data/raw/megafon_raw').touch()

    return f'Successfully fetched Megafon dataset ({num_rows} rows).' 


def make_datasets(dataset_names: str = 'All'):

    # Load Environment variables    
    load_dotenv(find_dotenv())

    # Dataset preparation functions
    make_dataset_fn = {
        'megafon': make_datasets_megafon
        }

    # Download all datasets
    if dataset_names == 'All':
        for fn in make_dataset_fn.values():
            msg = fn()
            print(msg)
    # Download particular dataset
    else:
        for name in dataset_names:
            fn = make_dataset_fn[name]
            msg = fn()
            print(msg)


# Main
if __name__ == '__main__':
    make_datasets()
