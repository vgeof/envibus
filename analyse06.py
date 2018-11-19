#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 18 18:44:30 2018

@author: vivien
"""

import pandas
from matplotlib import pyplot as plt
import numpy as np

columns = ['idscrap','time_saved','minutes','theorique']

data = pandas.read_csv('data200.csv',usecols = columns,parse_dates=['time_saved'])

def identifie_bus(data):
    data['bus_id'] = pandas.Series(0,index = data.index)
    data2 = data.copy()
    
    while len(data2)!=0:
        data2['rank'] = data2.groupby('idscrap')['minutes'].rank(ascending=True)
        cur = data2.loc[data2['rank']==1]
        bus_id = data['bus_id'].max() + 1
        if len(cur>2):
            while cur.iloc[0]['minutes']>2 or cur.iloc[1]['minutes']-cur.iloc[0]['minutes']<2\
            or  (cur.iloc[1]['time_saved']-cur.iloc[0]['time_saved']).seconds<60*cur.iloc[0]['minutes']:
                data.loc[data['time_saved']==cur.iloc[0]['time_saved'],'bus_id'] = bus_id
                data2 = data2.drop(cur.index[0])
                cur = cur.drop(cur.index[0])
                if len(cur) == 1 or cur.iloc[1]['minutes']-cur.iloc[0]['minutes']>2\
                or  (cur.iloc[1]['time_saved']-cur.iloc[0]['time_saved']).seconds>60*cur.iloc[0]['minutes']: 
                      data.loc[data['time_saved']==cur.iloc[0]['time_saved'],'bus_id'] = bus_id
    
                      data2 = data2.drop(cur.index[0])
                      cur = cur.drop(cur.index[0])
    
                      break
            print('bus added ' )

    
    return data

def plot():
    for i in list(set(data['bus_id'])):
        slic = data.loc[data['bus_id']==i]
        plt.plot_date(slic['time_saved'],slic['minutes'],'+',label=str(i))


data = identifie_bus(data)


def f(x):
    return x['time_saved'].max()
arrives = data.groupby('bus_id')['minutes'].min()<2
heures = data.groupby('bus_id')['time_saved'].max()
def summarize(index):
    if arrives.loc[index]: return (index,heures[index])
    else : return (index,None)
a = data.groupby('bus_id').agg(f)
arrivees = pandas.DataFrame(list(arrives.index.map(summarize)),columns = ['bus_id','arrivees'])
data = data.merge(arrivees,on = 'bus_id')


data = data.loc[-data['arrivees'].isnull()]
data['real_minutes'] = data['arrivees'] - data['time_saved']
data['real_minutes'] = data['real_minutes'].map(lambda x:x.seconds/60)

for name,bus in data.groupby('bus_id'):
    print(np.polyfit(bus['real_minutes'],bus['minutes'],deg = 1))

    

    




