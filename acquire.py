import env

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

# DBSCAN import
from sklearn.cluster import DBSCAN

# Scaler import
from sklearn.preprocessing import MinMaxScaler

def acquire_data():
    '''
    
    '''

    # saving url string to access sequel ace curriculum logs db
    url = f'mysql+pymysql://{env.user}:{env.password}@{env.host}/curriculum_logs'

    # saving query to access corhorts and logs tables
    query_cohorts = '''
    SELECT  id as cohortid_cohorts,
            start_date,
            end_date,
            name as cohortname,
            program_id
    FROM cohorts
    '''

    query_logs = '''
    SELECT *
    FROM logs
    '''

    # reading cohorts query to df
    df_cohorts = pd.read_sql(query_cohorts, url)

    # reading logs query to df
    df_logs = pd.read_sql(query_logs, url)

    # print statements for size of each df
    print(f'The cohorts df has {df_cohorts.shape[0]} rows and {df_cohorts.shape[1]} columns.')
    print(f'The logs df has {df_logs.shape[0]} rows and {df_logs.shape[1]} columns.')

    # joining the dfs with merge on cohort id, dropping duplicate float column
    df = df_cohorts.merge(df_logs, left_on = 'cohortid_cohorts', right_on = 'cohort_id')\
                                            .drop(columns = 'cohort_id')

    return df