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

data = pandas.read_csv('data200current.csv',usecols = columns,parse_dates=['time_saved'])
data = data.loc[data['theorique']==False]
#data = data.loc[data['time_saved']>="2018-11-19 18:51:03.401308"]
data = data.loc[data['time_saved']<="2018-11-20 8:51:03.401308"]

plt.plot_date('time_saved','minutes','+',data = data)
data['rank'] = data.groupby('idscrap')['minutes'].rank(ascending=True,method = 'first')
def identifie_bus(data):
    data['bus_id'] = pandas.Series(0,index = data.index)
    data2 = data.copy()
    
    while len(data2)!=0:
        data2['rank'] = data2.groupby('idscrap')['minutes'].rank(ascending=True,method = 'first')
        cur = data2.loc[data2['rank']==1]
        bus_id = data['bus_id'].max() + 1
        if len(cur>2):
            while cur.iloc[0]['minutes']>1 or cur.iloc[1]['minutes']-cur.iloc[0]['minutes']<1\
            or  (cur.iloc[1]['time_saved']-cur.iloc[0]['time_saved']).seconds<60*cur.iloc[0]['minutes']:
#                plt.clf()
#                plot()
#                plt.pause(0.05)
                data.loc[cur.iloc[0].name,'bus_id'] = bus_id
                data2 = data2.drop(cur.index[0])

                #input("Press")
                cur = cur.drop(cur.index[0])
                if len(cur) == 1 or (cur.iloc[0]['minutes'] == 1 and cur.iloc[1]['minutes']-cur.iloc[0]['minutes']>1) \
                or  ((cur.iloc[1]['time_saved']-cur.iloc[0]['time_saved']).seconds>60*cur.iloc[0]['minutes'] and cur.iloc[0]['minutes']>0): 
                      data.loc[cur.iloc[0].name,'bus_id'] = bus_id    
                      data2 = data2.drop(cur.index[0])
                      cur = cur.drop(cur.index[0])
    
                      break

            print('bus added ' )

    
    return data

def plot(key = 'bus_id'):
    for i in list(set(data[key])):
        slic = data.loc[data[key]==i]
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

#for name,bus in data.groupby('bus_id'):
#    if name == 1:
#        reg = np.polyfit(bus['real_minutes'],bus['minutes'],deg = 1)
#        print(reg)
#        x = bus['real_minutes']
#        plt.plot(x,bus['minutes'])
#        plt.plot(x,reg[0]*x + reg[1])
#plt.plot(x,x)

bus = data.loc[data['bus_id']==3]
x0 = 1
xn = x0
xs = [x0]
ds = []

for ind in (range(len(bus)-1)):
    dt = (bus.iloc[ind+1]['time_saved']-bus.iloc[ind]['time_saved'] ).seconds
    trn = bus.iloc[ind]['minutes']*60
    #trn = (bus.iloc[0]['minutes']*60-1*ind*dt)
    xn1 = xn*(1-dt/trn)
    ds.append((xn1-xn)/dt)
    xn = xn1
    print((1-dt/trn))
    xs.append(xn1)
    
plt.plot(xs)
plt.figure()
plt.plot(bus['minutes'])
rec = []
for i in range(0,len(xs)-1):
    rec.append(-xs[i]/ds[i])
    
#plt.plot(rec)
#plt.figure()
#
#plt.plot(bus['minutes'])
