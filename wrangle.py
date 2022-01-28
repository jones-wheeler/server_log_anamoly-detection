import numpy as np
import pandas as pd

import os
import env

####### Acquire #######
def acquire_data():
    '''
    This function acquires curriculum access logs from the codeup database
    and returns a dataframe
    '''

    # saving url string to access sequel ace curriculum logs db
    url = f'mysql+pymysql://{env.user}:{env.password}@{env.host}/curriculum_logs'

    # saving query to access corhorts and logs tables
    query_cohorts = '''
    SELECT  id as cohortid,
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

    # joining the dfs with merge on cohort id, dropping duplicate float column
    df = df_cohorts.merge(df_logs, left_on = 'cohortid', right_on = 'cohort_id')\
                                            .drop(columns = 'cohort_id')

    return df

######## Prepare #######
def prepare_logs(df):
    '''
    This function takes in a dataframe and creates features to assist in exploration,
    drops null value, creates datetime features.
    '''
    conditions = [df.program_id == 1, df.program_id == 2, df.program_id == 3, df.program_id == 4]
    result = ['web_dev','web_dev','data_science','web_dev']
    df['program_name'] = np.select(conditions, result)
    df['is_staff'] = df.isin(df[df.cohortname == 'Staff']).cohortname
    df.dropna
    df['date'] = pd.to_datetime(df.date)
    df['end_date'] = pd.to_datetime(df.end_date)
    df['start_date'] = pd.to_datetime(df.start_date)
    # df['time'] = pd.to_datetime(df.time, format='%H:%M:%S')
    return df

####### Wrangle #######
def get_curriculum_logs_data():
    '''
    This function reads in curriculum logs data from the Codeup database, 
    writes data to a csv file if a local file does not exist, and returns a df.
    '''
    if os.path.isfile('curriculum_logs.csv'):
        
        # If csv file exists read in data from csv file.
        df = pd.read_csv('curriculum_logs.csv', index_col=0)      
        # print statements for size of each df
        print(f'The df has {df.shape[0]} rows and {df.shape[1]} columns.')

    else:
        
        # Read fresh data from db into a DataFrame
        df = acquire_data()

        # Cache data
        df.to_csv('curriculum_logs.csv')


        # print statements for size of each df
        print(f'The df has {df.shape[0]} rows and {df.shape[1]} columns.')

    # prepare data
    df = prepare_logs(df)
    
    return df




