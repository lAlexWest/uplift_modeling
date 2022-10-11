import pandas as pd
import numpy as np
import sqlite3

# Processing functions
from transformation import transform_megafon

# Load environment variables
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import os


def build_features_megafon():
    
    # SQLite connection and Project directory
    project_dir = Path(__file__).resolve().parents[2]
    conn = sqlite3.connect(os.environ['DATABASE_URL'])


    # Read raw data
    query = 'SELECT * FROM megafon_raw'
    data = pd.read_sql(query, conn)

    # Transform data
    data = data.pipe(transform_megafon)
    
    # Store transformed data
    chunksize = int(np.ceil(32766 / len(data.columns)))
    num_rows = data.to_sql(
        'megafon_interim', conn, if_exists='replace', index=False, 
        chunksize=chunksize, method='multi'
        )
    Path(f'{project_dir}/data/interim/megafon_intrim').touch()

    # Build features

    # TBD
    # 
    #
    #

    # Store processed data (the final, canonical data sets for modeling)
    query = '''
    CREATE VIEW megafon_processed AS
        SELECT * FROM megafon_interim
    '''
    conn.execute(query).fetchall()
    Path(f'{project_dir}/data/interim/megafon_processed').touch()
    conn.close()
    return 'Successfully prepared megafon dataset for modeling'
    


def build_features(dataset_names: str = 'All'):

    # Load Environment variables
    load_dotenv(find_dotenv())

    # Dataset preparation functions
    build_features_fn = {
        'megafon': build_features_megafon
        }

    # Download and tide all datasets
    if dataset_names == 'All':
        for fn in build_features_fn.values():
            msg = fn()
            print(msg)
    # Download and tide particular dataset
    else:
        for name in dataset_names:
            fn = build_features_fn[name]
            msg = fn()
            print(msg)



if __name__ == '__main__':

    build_features()