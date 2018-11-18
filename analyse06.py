#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 18 18:44:30 2018

@author: vivien
"""

import pandas
from matplotlib import pyplot as plt

columns = ['idscrap','time_saved','minutes','theorique']

#data = pandas.read_csv('data200.csv',usecols = columns,parse_dates=['time_saved'])
data['bus_id'] = pandas.Series(0,index = data.index)
#plt.plot_date(data['time_saved'],data['minutes'],'+')

buses = []
#for i in range(1,data['idscrap'].max()-1):
#
#    current = data.loc[data['idscrap']==i]
#    previous = data.loc[data['idscrap']==i-1]
#    suivant = data.loc[data['idscrap']==i+1]
#    for classement in range(len(current)):
        
#
#current = data.loc[data['idscrap']==1]
#    for classement in range(len(current)):
#        complete = False
#        while not complete:
#            
            

data2 = data.copy()

while len(data2)!=0:
    data2['rank'] = data2.groupby('idscrap')['minutes'].rank(ascending=True)
    cur = data2.loc[data2['rank']==1]
    i = 0
    current_bus = []
    bus_id = data['bus_id'].max() + 1
    if len(cur>2):
        while cur.iloc[0]['minutes']>2 or cur.iloc[1]['minutes']-cur.iloc[0]['minutes']<2:
            data.loc[data['time_saved']==cur.iloc[0]['time_saved'],'bus_id'] = bus_id
            data2 = data2.drop(cur.index[0])
            cur = cur.drop(cur.index[0])
            print(cur)
            if len(cur) == 1 or cur.iloc[1]['minutes']-cur.iloc[0]['minutes']>2: 
                  data.loc[data['time_saved']==cur.iloc[0]['time_saved'],'bus_id'] = bus_id

                  data2 = data2.drop(cur.index[0])
                  cur = cur.drop(cur.index[0])

                  break


    
#        data.loc[data['time_saved']==cur.iloc[0]['time_saved'],'bus_id'] = bus_id
#        data2 = data2.drop(cur.index[0])
    print('bus added ' )

for i in list(set(data['bus_id'])):
    slic = data.loc[data['bus_id']==i]
    plt.plot_date(slic['time_saved'],slic['minutes'],'+')
    
    




