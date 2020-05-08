# Import packages
import io
import os
import re
import csv
import time
import json
import copy
import gzip
import struct

import boto3
import botocore 

import numpy as np
import pandas as pd
pd.reset_option('max_columns')

from time import gmtime, strftime
from sagemaker import get_execution_role
role = get_execution_role()

from IPython.display import clear_output

# Define functions
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def make_sqft_dict():
    dict_sqft = {
        **dict(enumerate(np.load('dict_1.npy').flatten()))[0],
        **dict(enumerate(np.load('dict_2.npy').flatten()))[0],
        **dict(enumerate(np.load('dict_3.npy').flatten()))[0],
        **dict(enumerate(np.load('dict_4.npy').flatten()))[0],
        **dict(enumerate(np.load('dict_5.npy').flatten()))[0],
        **dict(enumerate(np.load('dict_6.npy').flatten()))[0],
        **dict(enumerate(np.load('dict_7.npy').flatten()))[0],
        **dict(enumerate(np.load('dict_8.npy').flatten()))[0],
        **dict(enumerate(np.load('dict_9.npy').flatten()))[0],
        **dict(enumerate(np.load('dict_10.npy').flatten()))[0]
    }
    return(dict_sqft)

def keep_if_sqft(df, dict_sqft):
    # Lookup square feet in dictionary
    df['sqft36'] = df.safegraph_place_id.apply(lambda row: (dict_sqft[row]/36) if row in dict_sqft else False)
    # Drop out those with missing square feet specifications
    NO_SQFT = len(df.loc[df.sqft36 == False])
    RECORDS = len(df)
    print(f'\t Dropping {NO_SQFT} out of {RECORDS} records because no SQFT.')
    df = df.loc[df.sqft36 != False]
    return(df)

def get_conacts(df):
    df = df[['safegraph_place_id','visits_by_each_hour','median_dwell','date_range_start','sqft36']].copy()
    # Generate list of visits by hour
    df['close_contacts'] = None
    # Turn string into a list of strings
    df.close_contacts = df.visits_by_each_hour.str.translate(str.maketrans({'[':'', ']':'','"':''})).str.split(',')
    # Turn list of strings into a list of ints with each int indicating visitors during each hour of the week
    df.close_contacts = df.apply(lambda row: [int(i) for i in row.close_contacts], axis=1)
    # Turn the list of ints into a chunks of 24 hours for each day and calculate the average interactions per 36 square feet
    df.close_contacts = df.apply(lambda row: [np.divide(\
                                              pd.DataFrame(c).rolling(int(max(row.median_dwell/60, 1)), min_periods=1).sum()[0].tolist()\
                                              , row.sqft36)\
                                              .mean()
                                              for c in chunks(row.close_contacts, 24)], axis=1)
    df = df.drop(['visits_by_each_hour','median_dwell','sqft36'], axis = 1)
    df = df.close_contacts.apply(pd.Series)\
         .merge(df, left_index=True, right_index=True)\
         .drop(['close_contacts'], axis=1)\
         .melt(id_vars=['safegraph_place_id','date_range_start'],value_name='visits')
    df['date'] = (pd.to_datetime(pd.to_datetime(df.date_range_start)\
                  .map(lambda x: x.strftime('%Y-%m-%d')))\
                  + pd.to_timedelta(pd.np.ceil(df.variable.astype(float)), unit="D"))
    df = df.drop(['variable'], axis = 1)
    return(df)

def get_blocks(df):
    df = df[['safegraph_place_id','date_range_start','visitor_home_cbgs']]
    df = df.visitor_home_cbgs\
        .str.translate(str\
        .maketrans({'{':'', '}':'','"':''}))\
        .str.split(',')\
        .apply(pd.Series)\
        .merge(df, left_index=True, right_index=True)\
        .drop(['visitor_home_cbgs'], axis=1)\
        .melt(id_vars=['safegraph_place_id','date_range_start'],value_name='cbgs')\
        .drop('variable', axis = 1)\
        .dropna()
    df = df.cbgs\
        .str.split(':')\
        .apply(pd.Series)\
        .merge(df, left_index=True, right_index=True)\
        .drop(['cbgs'], axis=1)\
        .rename(columns={0:'cbsa',1:'party'})
    return(df)

def get_firms(df):
    df = df[['safegraph_place_id','location_name','brands','naics_code']].drop_duplicates()
    return(df)

def perform_calculations():
    
    print(f'Initializing...')

    # Set s3 bucket resource parameters
    s3 = boto3.Session().resource('s3')

    # Import dictionary of square footage
    dict_sqft = make_sqft_dict()

    # Set columns to keep
    COLS = ['safegraph_place_id','location_name',
            'brands','naics_code','date_range_start',
            'visits_by_each_hour','median_dwell',
            'visitor_home_cbgs']

    SAFEGRAPH_FILES = ['s3://safegraph-bucket-1/2020-03-01-weekly-patterns.csv.gz',
                       's3://safegraph-bucket-1/2020-03-08-weekly-patterns.csv.gz',
                       's3://safegraph-bucket-1/2020-03-15-weekly-patterns.csv.gz',
                       's3://safegraph-bucket-1/2020-03-22-weekly-patterns.csv.gz',
                       's3://safegraph-bucket-1/2020-03-29-weekly-patterns.csv.gz',
                       's3://safegraph-bucket-1/2020-04-05-weekly-patterns-corrected.csv.gz',
                       's3://safegraph-bucket-1/2020-04-12-weekly-patterns.csv.gz',
                       's3://safegraph-bucket-1/2020-04-19-weekly-patterns.csv.gz',
                       's3://safegraph-bucket-1/2020-04-26-weekly-patterns.csv.gz']
    
    clear_output(wait=True)
    
    for FILE in SAFEGRAPH_FILES[0:1]:

        counter = 0

        week = str(FILE[24:34])

        print(week)

        for chunk in pd.read_csv(FILE, usecols=COLS, compression='gzip', chunksize=10000):

            # Check if chunk SafeGraph ID exists in the SQFT dictionary
            chunk = keep_if_sqft(chunk, dict_sqft)

            print(f'Processing chunk {str(counter)}...')

            print('\t Extract Census blocks.')
            df_blocks = get_blocks(chunk)

            print('\t Extract firm detail.')
            df_firms = get_firms(chunk)

            print('\t Extract number of close contacts.')
            df_contacts = get_conacts(chunk)

            print('\t Merge data.')
            df = df_contacts.merge(df_blocks, on=['safegraph_place_id','date_range_start'])
            df = df.merge(df_firms, left_on='safegraph_place_id', right_on='safegraph_place_id')    

            df['fips'] = df.cbsa.str[0:5]
            df['inter_sum'] = df.visits.astype(float)
            df['inter_product'] = df.party.astype(float) * df.visits.astype(float)

            # Save by NAICS
            print('\t Save by NAICS.')
            df = df[['date','naics_code','fips','inter_sum','inter_product']]
            df = df[(df.fips == df.fips)&(df.fips!='')]
            df = df.groupby(['date', 'naics_code','fips'])['inter_sum','inter_product'].agg('sum')
            df.to_csv(f'temp/temp_naics_{week}.csv')

            # Save by all business categories  
            print('\t Save for all categories.')
            df = df.reset_index()
            df = df[['date','fips','inter_sum','inter_product']]
            df = df.groupby(['date','fips'])['inter_sum','inter_product'].agg('sum')
            df.to_csv(f'temp/temp_all_{week}.csv')

            # Upload to s3 bucket
            print('\t Upload to s3 bucket.')
            s3.meta.client.upload_file(f'temp/temp_all_{week}.csv', 'interactions-all-bucket', f'ci_all_{week}_chunk_{str(counter)}.csv')
            s3.meta.client.upload_file(f'temp/temp_naics_{week}.csv', 'interactions-naics-bucket', f'ci_naics_{week}_chunk_{str(counter)}.csv')    
            print(f'Processing chunk {str(counter)} complete.')

            counter = counter + 1
            clear_output(wait=True)

        print('Done.')
		

# Uncomment this line to perform calculations
# perform_calculations()