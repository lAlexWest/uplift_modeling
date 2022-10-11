import pandas as pd
import numpy as np


def transform_megafon(df):

    '''Megafon raw data transformation for feature engineeging stage'''

    # Treatment indicator to numerical
    df['treatment_group'] = df['treatment_group'].eq('treatment').astype('int')
    
    # Sort Features
    not_features = ['treatment_group', 'conversion']
    features = df.columns.difference(not_features).to_list()
    features = sorted(features, key=lambda x: int(x.lstrip('X_')))
    df = df.loc[:, features+not_features]

    return df