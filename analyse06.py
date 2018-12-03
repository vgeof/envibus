#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 18 18:44:30 2018

@author: vivien
"""

import pandas
from matplotlib import pyplot as plt
import numpy as np
from sklearn.linear_model import *
import random

columns = ['idscrap','time_saved','minutes','theorique','bus_id','real_minutes','arrivees']

data = pandas.read_csv('./data_enriched/data0.csv',usecols = columns,parse_dates=['time_saved','arrivees'])
#data = data.loc[data['theorique']==True]
data = data.loc[data['time_saved']>="2018-11-22 18:51:03.401308"]
#data = data.loc[data['time_saved']<="2018-11-20 8:51:03.401308"]

#plt.plot_date('time_saved','minutes','+',data = data)
data = data.loc[-data['arrivees'].isnull()]
bus_ids = list(set(data['bus_id']))
bus = data.loc[data['bus_id']==bus_ids[0]]
x0 = 1
xn = x0
xs = [x0]
ds = []
t = (bus['time_saved'] - bus['time_saved'].min()).map(lambda x : x.seconds)
for ind in (range(len(bus)-1)):
    dt = (bus.iloc[ind+1]['time_saved']-bus.iloc[ind]['time_saved'] ).seconds
    trn = bus.iloc[ind]['minutes']*60
    #trn = (bus.iloc[0]['minutes']*60-1*ind*dt)
    xn1 = xn*(1-dt/trn)
#    ds.append((xn1-xn)/dt)
    xn = xn1
#    print((1-dt/trn))
    xs.append(xn1)
newt = range(0,max(t),30)
newx = np.interp(newt,t,xs)
newx = list(map(lambda x: x+random.gauss(0,0.01),newx))
trec = []
#plt.plot(t,xs,'+')
#plt.plot(newt,newx,'p')
for ind in range(len(newt)-1):
    dt = 30
    ds.append((newx[ind+1] - newx[ind])/dt)
    trec.append(-newx[ind]/ds[ind])

plt.plot(t,bus['minutes']*60)
plt.plot(newt[:-1],trec)
#plt.figure()
#plt.plot(bus['minutes'])
#rec = []
#for i in range(0,len(xs)-1):
#    rec.append(-xs[i]/ds[i])
#    rl = Ridge(0,False)
#
#final = []
#for name,bus in data.groupby('bus_id'):
#    if len(bus) >=10:
##        reg = np.polyfit(bus['real_minutes'],bus['minutes'],deg = 1)
#        rl.fit(np.array(bus['real_minutes']).reshape(-1, 1),bus['minutes'])
#        print(rl.coef_)
#        x = bus['real_minutes']
##        plt.plot(x,bus['minutes'])
##        plt.plot(x,reg[0]*x + reg[1])
##        plt.plot(x,rl.coef_[0]*x)
#        final.append([name,bus.iloc[0]['arrivees'],rl.coef_[0]])
#d = pandas.DataFrame(final)
#d[1] = d[1].map(lambda x : x.hour + x.minute/60)
#plt.plot(d[1],d[2],'+')
##plt.plot(x,x)
#
#    
#plt.plot(rec)
#plt.figure()
#
#plt.plot(bus['minutes'])
