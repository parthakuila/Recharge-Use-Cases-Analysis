#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 10:59:14 2018

@author: partha
"""

import pandas as pd
import numpy as np
import datetime as dt
import datetime
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import style 
%matplotlib inline
import os
import logging
from statistics import stdev 
from statsmodels.api import add_constant

## Import data
rch_df=pd.read_csv("/home/partha/Documents/Recharge_Case/cust_daily_usage_2018_merged_Dhaka_new.csv",names=('subscriber_id','Date','Usage'))
rch_df=pd.read_csv("/home/partha/Documents/Recharge_Case/cust_daily_usage_2018_merged_CTG_new.csv", names=('subscriber_id','Date','Usage'))
rch_df=pd.read_csv("/home/partha/Documents/Recharge_Case/cust_daily_usage_2018_merged_Sylhet_new.csv", names=('subscriber_id','Date','Usage'))

rch_df.head()
rch_df.shape
rch_df.dtypes

## Check there is null in data set
rch_df.isnull().sum()

## converted 'Usage' byte to gb
#rch_df['Usage'] = rch_df['Usage'].astype('int64')
#rch_df.Usage = int(rch_df['Usage'], 2)
rch_df['Usage'] = rch_df.Usage/(1024*1024*1024)   # in gb

## Converted dated
#import datetime

rch_df['Date'] = sorted(rch_df['Date'])
#rch_df['Day'] = rch_df['Date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').strftime('%A'))
#rch_df['Day'] = rch_df['Date'].apply(lambda x: datetime.strptime(x, '%Y%m%d%H'))
rch_df['Date'] = pd.to_datetime(rch_df.Date)

## Create monthly Usage every subscriber #=======================================================================
#rch_df.resample('m', on='Date')['Usage'].sum().dropna()
#rch_df.groupby(['subscriber_id','Date']).resample('m', on='Date')['Usage'].sum().dropna()
#rch_df.groupby(['subscriber_id','Date']).sum(by = 'm')['Usage'].dropna()
rch_df_monthly = rch_df.set_index('Date').groupby('subscriber_id').Usage.resample('M', 'sum')
rch_df_monthly = rch_df_monthly.to_frame().reset_index()
rch_df_monthly.head()
rch_df_monthly.shape
rch_df_monthly.dtypes
#cols = rch_df_monthly.columns[rch_df_monthly.dtypes.eq(object)]
#rch_df_monthly[cols] = rch_df_monthly[cols].apply(pd.to_numeric, errors='coerce', axis=0)
rch_df_monthly['Usage']= pd.to_numeric(rch_df_monthly.Usage, errors='coerce')
rch_df_monthly.columns = ['Usage(GB)' if x=='Usage' else x for x in rch_df_monthly.columns]
rch_df_monthly.isnull().sum()
rch_df_monthly.dropna(subset=['Usage(GB)'], inplace=True)
rch_df_monthly['subscriber_id'].nunique()


## Create Specific Bucket for monthly usage for every subscriber #===============================================
## Create function for Bucketting ##=============================================================================
def Monthly_Bucket(row):
    if((int(row['Usage(GB)'])>0) & (int(row['Usage(GB)'])<=30)):
        return '(0-30)gb'
    elif((int(row['Usage(GB)'])>30) & (int(row['Usage(GB)'])<=60)):
        return '(30-60)gb'
    elif((int(row['Usage(GB)'])>60) & (int(row['Usage(GB)'])<= 90)):
        return '(60-90)gb'
    elif((int(row['Usage(GB)'])>90) & (int(row['Usage(GB)'])<= 120)):
        return '(90-120)gb'
    elif((int(row['Usage(GB)'])>120) & (int(row['Usage(GB)'])<= 150)):
        return '(120-150)gb'
    elif((int(row['Usage(GB)'])>150) & (int(row['Usage(GB)'])<= 180)):
        return '(150-180)gb'
    else:
        return 'more than 180 gb'


rch_df_monthly['Monthly_Bucket']='more than 180 gb'
rch_df_monthly.loc[(rch_df_monthly['Usage(GB)'] > 0) & (rch_df_monthly['Usage(GB)'] <= 30),'Monthly_Bucket'] = '(0-30)gb'
rch_df_monthly.loc[(rch_df_monthly['Usage(GB)'] > 30) & (rch_df_monthly['Usage(GB)'] <= 60),'Monthly_Bucket'] = '(30-60)gb'
rch_df_monthly.loc[(rch_df_monthly['Usage(GB)'] > 60) & (rch_df_monthly['Usage(GB)'] <= 90),'Monthly_Bucket'] = '(60-90)gb'
rch_df_monthly.loc[(rch_df_monthly['Usage(GB)'] > 90) & (rch_df_monthly['Usage(GB)'] <= 120),'Monthly_Bucket'] = '(90-120)gb'
rch_df_monthly.loc[(rch_df_monthly['Usage(GB)'] > 120) & (rch_df_monthly['Usage(GB)'] <= 150),'Monthly_Bucket'] = '(120-150)gb'
rch_df_monthly.loc[(rch_df_monthly['Usage(GB)'] > 150) & (rch_df_monthly['Usage(GB)'] <= 180),'Monthly_Bucket'] = '(150-180)gb'


#rch_df_monthly = rch_df_monthly.rename(columns={'Usage': 'Usage_GB'}, inplace=True)
#gtpu_daily_agg.to_csv(write_dir +  "gtpu_daily_agg_" + dated + ".csv.gz", sep = ",", index = False, compression="gzip", float_format='%.5f')#,encoding = "utf-8")
#rch_df_monthly.to_csv( "CTG_monthly_recharge_cases",".csv", sep = ",", index = False,float_format='%.5f')#,encoding = "utf-8")
#rch_df_monthly['Usage(GB)'].unstack([0,1]).plot()
rch_df_monthly.to_csv("Sylhet_monthly_bucketing_recharge_cases", + ".csv", sep =",", encoding='utf-8', index=False)


