#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 18:49:29 2018

@author: vivien
"""

import pandas 
import matplotlib.pyplot as plt
import datetime
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
from sklearn.linear_model import LinearRegression

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
    x0 = np.interp(15,dd.index,dd)
    dd = dd-x0
    x2 = first +datetime.timedelta(0,-x0 * 60)
    params = list(range(15,20))
    x1 = np.interp(params,list(dd.index),list(dd))
    y =  np.interp(0,dd.index,dd)

    return (x0,x1,x2,y)
    
#df.groupby('bus_id').apply(transform,axis = 1)
    
def calcul(x,cs):
    ret = cs[0]*np.power(x,len(cs)-1)
    j = len(cs)-1
    for i in range(1, len(cs)):
        j-=1
      
        ret += c[i]*np.power(x,j)
    return ret

#
#for name,bus in df.groupby('bus_id'):
#    if name%3 ==0:
#        c = transform(bus)
#        plt.plot(x,c)
#        plt.plot(bus['minutes'],bus['diff'],)
d = df.groupby('bus_id').apply(transform)
y = d.map(lambda x: x[3])
X = pandas.DataFrame(list(d.map(lambda x : x[1])),d.index)
X['day'] = d.map(lambda x: x[2].dayofweek)
X['hour'] = d.map(lambda x: (x[2]-datetime.datetime(x[2].year,x[2].month,x[2].day,17, 0, 0, 436453)).total_seconds()/60)

X =X.loc[abs(X.hour)<2*60] 
y = y.loc[X.index]
X['hour2'] = X.hour.apply(lambda x : x**2)
X['hour3'] = X.hour.apply(lambda x : x**3)
X['mer'] = X.day==2
X['lun'] = X.day==0
X = X[['hour','hour2','mer','lun']]
Xtrain,Xtest,ytrain,ytest = train_test_split(X,y,test_size = 0.3)
#clf = MLPRegressor((100,100),max_iter = 500,verbose = True)
clf = LinearRegression()
clf.fit(Xtrain,ytrain)
print(sum(abs(clf.predict(Xtest) - ytest))/len(ytest))
print(clf.score(Xtrain,ytrain))
print(clf.score(Xtest,ytest))