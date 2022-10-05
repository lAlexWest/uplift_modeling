from importlib import find_loader
import os
from xml.dom import NotFoundErr
from dotenv import find_dotenv, load_dotenv

import numpy as np
import pandas as pd
import sqlite3

from sklift import datasets
from pathlib import Path


def load_and_process_megafon():
    '''Loads and processes Megafon dataset'''    

    def process(df):

        # Treatment indicator to numerical
        df['treatment_group'] = df['treatment_group'].eq('treatment').astype('int')
        
        # Sort Features
        not_features = ['treatment_group', 'conversion']
        features = df.columns.difference(not_features).to_list()
        features = sorted(features, key=lambda x: int(x.lstrip('X_')))
        df = df.loc[:, features+not_features]

        return df
    
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

    # Process data
    df = df.pipe(process)
    
    # Store processed_data
    num_rows = df.to_sql(
        'megafon_processed', conn, if_exists='replace', index=False, 
        chunksize=chunksize, method='multi'
        )
    conn.close()
    Path(f'{project_dir}/data/processed/megafon_processed').touch()

    return f'Successfully downloaded and tided Megafon dataset ({num_rows} rows).' 


def download_and_tide(dataset_names='All'):

    # Load Environment variables    
    load_dotenv(find_dotenv())

    # Dataset preparation functions
    create_dataset_fn = {
        'megafon': load_and_process_megafon
        }

    # Download and tide all datasets
    if dataset_names == 'All':
        for fn in create_dataset_fn.values():
            msg = fn()
    # Download and tide particular dataset
    else:
        for name in dataset_names:
            fn = create_dataset_fn[name]
            msg = fn()


# Main
if __name__ == '__main__':
    download_and_tide()
