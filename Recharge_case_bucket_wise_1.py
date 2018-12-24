#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 13 14:46:10 2018

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
#rch_df=pd.read_csv("/home/partha/Documents/Recharge_Case/cust_daily_usage_2018_merged_Dhaka_new.csv",names=('subscriber_id','Date','Usage'))
rch_df=pd.read_csv("/home/partha/Documents/Recharge_Case/cust_daily_usage_2018_merged_CTG_new.csv", names=('subscriber_id','Date','Usage'))

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
rch_df['Day'] = rch_df['Date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').strftime('%A'))
#rch_df['Day'] = rch_df['Date'].apply(lambda x: datetime.strptime(x, '%Y%m%d%H'))
#rch_df['Date'] = pd.to_datetime(rch_df.Date,format='%d/%m/%Y')
workingday=['Sunday','Monday','Tuesday','Wednesday','Thursday']
weekend=['Friday','Saturday']
## Create function for Workingday and weekend 
def weekday(row):
     if((int(row['Day']) == 'Sunday')):
        return 'workingday'
     elif((int(row['Day']) == 'Monday')):
        return 'workingday'
     elif((int(row['Day']) =='Tuesday')):
        return 'workingday'
     elif((int(row['Day']) == 'Wednesday')):
        return 'workingday'
     elif((int(row['Day']) == 'Thursday')):
        return 'workingday'
     else:
        return 'weekend'
    
rch_df['weekday'] = 'weekend'
rch_df.loc[(rch_df['Day'] == 'Sunday'),'weekday'] = 'workingday'
rch_df.loc[(rch_df['Day'] == 'Monday'),'weekday'] = 'workingday'
rch_df.loc[(rch_df['Day'] == 'Tuesday'),'weekday'] = 'workingday'
rch_df.loc[(rch_df['Day'] == 'Wednesday'),'weekday'] = 'workingday'
rch_df.loc[(rch_df['Day'] == 'Thursday'),'weekday'] = 'workingday'

## Create function for Usage Bucket per Day wise
def Bucket(row):
    if((int(row['Usage'])>0) & (int(row['Usage'])<=1)):
        return 'Dly_usage(0-1)gb'
    elif((int(row['Usage'])>1) & (int(row['Usage'])<=2)):
        return 'Dly_usage(1-2)gb'
    elif((int(row['Usage'])>2) & (int(row['Usage'])<= 3)):
        return 'Dly_usage(2-3)gb'
    elif((int(row['Usage'])>3) & (int(row['Usage'])<= 4)):
        return 'Dly_usage(3-4)gb'
    elif((int(row['Usage'])>4) & (int(row['Usage'])<= 5)):
        return 'Dly_usage(4-5)gb'
    elif((int(row['Usage'])>5) & (int(row['Usage'])<= 7)):
        return 'Dly_usage(5-7)gb'
    elif((int(row['Usage'])>7) & (int(row['Usage'])<= 9)):
        return 'Dly_usage(7-9)gb'
    elif((int(row['Usage'])>9) & (int(row['Usage'])<= 11)):
        return 'Dly_usage(9-11)gb'
    elif((int(row['Usage'])>11) & (int(row['Usage'])<= 14)):
        return 'Dly_usage(11-14)gb'
    elif((int(row['Usage'])>14) & (int(row['Usage']) <= 17)):
        return 'Dly_usage(14-17)gb'
    
    elif((int(row['Usage'])>17) & (int(row['Usage'])<=20)):
        return 'Dly_usage(17-20)gb'
    elif((int(row['Usage'])>20) & (int(row['Usage'])<= 25)):
        return 'Dly_usage(20-25)gb'
    elif((int(row['Usage'])>25) & (int(row['Usage'])<= 30)):
        return 'Dly_usage(25-30)gb'
    elif((int(row['Usage'])>30) & (int(row['Usage'])<= 35)):
        return 'Dly_usage(30-35)gb'
    elif((int(row['Usage'])>35) & (int(row['Usage'])<= 40)):
        return 'Dly_usage(35-40)gb'
    elif((int(row['Usage'])>40) & (int(row['Usage'])<= 45)):
        return 'Dly_usage(40-45)gb'
    elif((int(row['Usage'])>45) & (int(row['Usage'])<= 50)):
        return 'Dly_usage(45-50)gb'
    elif((int(row['Usage'])>50) & (int(row['Usage'])<= 60)):
        return 'Dly_usage(50-60)gb'
    elif((int(row['Usage'])>60) & (int(row['Usage'])<= 70)):
        return 'Dly_usage(60-70)gb'
    elif((int(row['Usage'])>70) & (int(row['Usage'])<= 80)):
        return 'Dly_usage(70-80)gb'
    elif((int(row['Usage'])>80) & (int(row['Usage'])<= 90)):
        return 'Dly_usage(80-90)gb'
    elif((int(row['Usage'])>90) & (int(row['Usage'])<= 100)):
        return 'Dly_usage(90-100)gb'
    
    else:
        return 'Dly_max_usage' 
    
    
    
rch_df['Bucket']='Dly_max_usage'
rch_df.loc[(rch_df['Usage'] > 0) & (rch_df['Usage'] <= 1),'Bucket'] = 'Dly_usage(0-1)gb'
rch_df.loc[(rch_df['Usage'] > 1) & (rch_df['Usage'] <= 2),'Bucket'] = 'Dly_usage(1-2)gb'
rch_df.loc[(rch_df['Usage'] > 2) & (rch_df['Usage'] <= 3),'Bucket'] = 'Dly_usage(2-3)gb'
rch_df.loc[(rch_df['Usage'] > 3) & (rch_df['Usage'] <= 4),'Bucket'] = 'Dly_usage(3-4)gb'
rch_df.loc[(rch_df['Usage'] > 4) & (rch_df['Usage'] <= 5),'Bucket'] = 'Dly_usage(4-5)gb'
rch_df.loc[(rch_df['Usage'] > 5) & (rch_df['Usage'] <= 7),'Bucket'] = 'Dly_usage(5-7)gb'
rch_df.loc[(rch_df['Usage'] > 7) & (rch_df['Usage'] <= 9),'Bucket'] = 'Dly_usage(7-9)gb'
rch_df.loc[(rch_df['Usage'] > 9) & (rch_df['Usage'] <= 11),'Bucket'] = 'Dly_usage(9-11)gb'
rch_df.loc[(rch_df['Usage'] > 11) & (rch_df['Usage'] <= 14),'Bucket'] = 'Dly_usage(11-14)gb'
rch_df.loc[(rch_df['Usage'] > 14) & (rch_df['Usage'] <= 17),'Bucket'] = 'Dly_usage(14-17)gb'
rch_df.loc[(rch_df['Usage'] > 17) & (rch_df['Usage'] <= 20),'Bucket'] = 'Dly_usage(17-20)gb'
rch_df.loc[(rch_df['Usage'] > 20) & (rch_df['Usage'] <= 25),'Bucket'] = 'Dly_usage(20-25)gb'
rch_df.loc[(rch_df['Usage'] > 25) & (rch_df['Usage'] <= 30),'Bucket'] = 'Dly_usage(25-30)gb'
rch_df.loc[(rch_df['Usage'] > 30) & (rch_df['Usage'] <= 35),'Bucket'] = 'Dly_usage(30-35)gb'
rch_df.loc[(rch_df['Usage'] > 35) & (rch_df['Usage'] <= 40),'Bucket'] = 'Dly_usage(35-40)gb'
rch_df.loc[(rch_df['Usage'] > 40) & (rch_df['Usage'] <= 45),'Bucket'] = 'Dly_usage(40-45)gb'
rch_df.loc[(rch_df['Usage'] > 45) & (rch_df['Usage'] <= 50),'Bucket'] = 'Dly_usage(45-50)gb'
rch_df.loc[(rch_df['Usage'] > 50) & (rch_df['Usage'] <= 60),'Bucket'] = 'Dly_usage(50-60)gb'
rch_df.loc[(rch_df['Usage'] > 60) & (rch_df['Usage'] <= 70),'Bucket'] = 'Dly_usage(60-70)gb'
rch_df.loc[(rch_df['Usage'] > 70) & (rch_df['Usage'] <= 80),'Bucket'] = 'Dly_usage(70-80)gb'
rch_df.loc[(rch_df['Usage'] > 80) & (rch_df['Usage'] <= 90),'Bucket'] = 'Dly_usage(80-90)gb'
rch_df.loc[(rch_df['Usage'] > 90) & (rch_df['Usage'] <= 100),'Bucket'] = 'Dly_usage(90-100)gb'

unique_subscriber_id = rch_df['subscriber_id'].unique()
rch_df.Bucket.value_counts()
Dly_max_usage = rch_df.loc[rch_df['Bucket']=='Dly_max_usage']
Dly_max_usage.groupby('weekday').Usage.count()

def subscriber_id(i):
## Per unique subscriber wise 'Usage'
   specific_subscriber = rch_df[rch_df.subscriber_id == unique_subscriber_id[1]]
   specific_subscriber['Date'] = sorted(specific_subscriber['Date'])
   specific_subscriber.drop_duplicates(['Date'],inplace=True)
   print('SubscriberID:', unique_subscriber_id[1])
   print('Total Number of days Usage:' ,len(specific_subscriber))
   print('Percentage of count and Usage within Bucket:', specific_subscriber.groupby('Bucket').sum().transform(lambda x: x/np.sum(x)*100))
   print(specific_subscriber.groupby('Bucket').agg(["count","mean"]))
   print('cv:', (specific_subscriber.groupby('Bucket').Usage.std()/specific_subscriber.groupby('Bucket').Usage.mean())*100)
   print('Bucketize b/w workingday and weekend:',specific_subscriber.groupby('weekday').agg(["count","mean"]))
   #print('cv:', (specific_subscriber.groupby('weekday').Usage.std()/specific_subscriber.groupby('weekday').Usage.mean())*100)
   print(specific_subscriber.groupby(['weekday','Bucket']).agg(['count','mean']))
   #print('cv:',(specific_subscriber.groupby(['weekday','Bucket']).Usage.std()/specific_subscriber.groupby(['weekday','Bucket']).Usage.mean())*100)
   #print(pd.crosstab(specific_subscriber['weekday'], specific_subscriber['Bucket']))

