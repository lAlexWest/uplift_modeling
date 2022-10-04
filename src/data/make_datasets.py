import numpy as np
import pandas as pd
import sqlite3

from sklift import datasets
from pathlib import Path


def load_and_process_megafon():
    '''Loads and processes Megafon dataset'''    

    def process(df):
        pass
        return df
    
    # SQLite connection
    conn = sqlite3.connect('data/lake.db')

    # Load all datasets and tide them and store to SQLlite
    megafon_data = datasets.fetch_megafon()
    df = pd.DataFrame(megafon_data['data'])

    # Concat to wide df
    df = pd.concat([df, megafon_data['target'].to_frame(), megafon_data['treatment']], axis=1)
    
    # Process
    df = df.pipe(process)
    
    # Store
    chunksize = int(np.ceil(32766 / len(df.columns)))
    num_rows = df.to_sql(
        'megafon_processed', conn, if_exists='replace', index=False, 
        chunksize=chunksize, method='multi'
        )
    conn.close()
    Path('data/processed/megafon_processed').touch()
    return f'Successfully downloaded and tided Megafon dataset ({num_rows} rows).' 


# Main
if __name__ == '__main__':

    create_dataset_fn = [load_and_process_megafon]
    for fn in create_dataset_fn:
        msg = fn()
        print(msg)
