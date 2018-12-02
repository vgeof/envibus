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
#data = data.loc[data['theorique']==True]
data = data.loc[data['time_saved']>="2018-11-22 18:51:03.401308"]
#data = data.loc[data['time_saved']<="2018-11-20 8:51:03.401308"]

#plt.plot_date('time_saved','minutes','+',data = data)
data['rank'] = data.groupby('idscrap')['minutes'].rank(ascending=True,method = 'first')


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
