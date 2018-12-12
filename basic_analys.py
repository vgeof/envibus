#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 18:49:29 2018

@author: vivien
"""

import pandas 
import matplotlib.pyplot as plt
import numpy as np

df = pandas.read_csv('./data_enriched/data2.csv',parse_dates = ['time_saved'])
df = df.loc[-pandas.isnull(df.arrivees)]
good_buses =  df.groupby('bus_id')['idscrap'].apply(lambda x: len(x))>30
good_buses = set(good_buses.loc[good_buses].index)
df = df.loc[df.bus_id.apply(lambda x: x in good_buses)]
firsts = df.groupby('bus_id')['time_saved'].apply(lambda x: x.min())
df['diff'] = df.apply(lambda x : ( x.loc['time_saved']- firsts.loc[x.bus_id]).seconds/-60, axis =1)

#df.groupby('bus_id')


x = np.linspace(0,30,100)


def transform(bus):
    first = bus.time_saved.min()
    bus.diff = bus.apply(lambda x: (x.time_saved - first).seconds/-60,axis = 1)
#    ret = np.polyfit(bus.minutes,diff,4)
    dd = bus.groupby('minutes')['diff'].mean()
    return np.interp(x,list(dd.index),list(dd))
    
#df.groupby('bus_id').apply(transform,axis = 1)
    
def calcul(x,cs):
    ret = cs[0]*np.power(x,len(cs)-1)
    j = len(cs)-1
    for i in range(1, len(cs)):
        j-=1
      
        ret += c[i]*np.power(x,j)
    return ret


for name,bus in df.groupby('bus_id'):
    if name%5 ==0:
        c = transform(bus)
        plt.plot(x,c)
#        plt.plot(bus['minutes'],bus['diff'],)