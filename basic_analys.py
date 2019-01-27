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
import sqlite3
import statsmodels.formula.api as smf
from statsmodels.sandbox.regression.predstd import wls_prediction_std


class compute:
    def __init__(self,arret = 490,filename = 'data.db',minutes_min = 15,nb_data_min = 30):
        self.nb_data_min = nb_data_min
        self.minutes_min = minutes_min
        self.con = sqlite3.connect(filename,detect_types = sqlite3.PARSE_COLNAMES)
        self.cursor = self.con.cursor()
        self.arret = arret
        
    def plot_bus(self,id_bus)    :
        self.cursor.execute("""SELECT BUS.id,minutes,time_saved as 'time_saved [timestamp]' FROM 
         BUS INNER JOIN SCRAPPED ON SCRAPPED.id = BUS.id
         WHERE id_arret = ? AND id_bus = ?
         """,(self.arret,id_bus))
        data = pandas.DataFrame(self.cursor.fetchall(),\
                                    columns = list(map(lambda x:x[0],self.cursor.description)))
        plt.plot_date(data['time_saved'],data['minutes'])
    def get_data(self):
        self.cursor.execute("""
                         SELECT BUS.id,
                         BUS.id_bus,
                         BUS_INFO.arrivee,
                         SCRAPPED.time_saved as 'time_saved [timestamp]',
                         SCRAPPED.minutes
                         FROM
                         BUS INNER JOIN SCRAPPED ON SCRAPPED.id = BUS.id
                         INNER JOIN BUS_INFO ON BUS.id_bus = BUS_INFO.id_bus
                         WHERE SCRAPPED.id_arret =?
                         AND BUS.id_bus<>-1
                         AND BUS_INFO.nb_data>=?
                         AND BUS_INFO.minutes_max>= ?
                         AND BUS_INFO.arrivee IS NOT NULL
                         """, (self.arret,self.nb_data_min,self.minutes_min))
        self.data = pandas.DataFrame(self.cursor.fetchall(),\
                                    columns = list(map(lambda x:x[0],self.cursor.description)))
        
    def process(self): 
        grid =  list(range(15,21))

        def fit_on_grid(bus):
            first = bus.time_saved.min()
            bus['diff'] = bus.apply(lambda x: (x.time_saved - first).seconds/-60,axis = 1)
            no_double = bus.groupby('minutes')['diff'].mean()
            if no_double.index.min()<min(grid) and no_double.index.max()>max(grid): # we ensure interp is not useless
                x1 = np.interp(grid,list(no_double.index),list(no_double))   
            else
            return list(map(lambda x:datetime.timedelta(0,-x*60)+first,x1))
        d = self.data.groupby('id_bus').apply(fit_on_grid)
        self.data_ready = pandas.DataFrame(d.tolist(),index = d.index,columns = grid)
      
        def calcul(x,cs):
            ret = cs[0]*np.power(x,len(cs)-1)
            j = len(cs)-1
            for i in range(1, len(cs)):
                j-=1
                ret += cs[i]*np.power(x,j)
            return ret
        self.data_ready['day'] = self.data_ready[15].map(lambda x: x.dayofweek)
        self.data_ready = self.data_ready.loc[self.data_ready['day'].map(lambda x: x in (0,1,2,3,4))]
        self.data_ready['hour'] =self.data_ready[15].map(lambda x: (x-datetime.datetime(x.year,x.month,x.day,17, 0, 0, 436453)).total_seconds()/60)
        self.data_ready['hour2'] =self.data_ready['hour'].map(lambda x : x**2)
        self.data_ready['hour'] = (self.data_ready.hour-self.data_ready.hour.mean())/(self.data_ready.hour.max()-self.data_ready.hour.min())
        self.data_ready['hour2'] = (self.data_ready.hour2-self.data_ready.hour2.mean())/(self.data_ready.hour2.max()-self.data_ready.hour2.min())
        self.data_ready = self.data_ready.loc[abs(self.data_ready['hour'])<2*60]
        
    def add_y(self):
        
        self.y = self.data.groupby('id_bus')['time_saved'].max()
        self.y = self.y.loc[self.data_ready.index]
        self.y = (self.y-self.data_ready[15]).map(lambda x:x.seconds/60)
        

class RegLin:
    def __init__(self,split = 0.3):
        self.split = split
            
    def train(self,X,y):
        if self.split:
            self. Xtrain,self.Xtest,self.ytrain,self.ytest = train_test_split(X,y,test_size = self.split)
        
        self.model = smf.ols('y ~ hour + hour2 +C(day)', data=self.Xtrain.join(self.ytrain.to_frame(name = 'y'))).fit()
    def test(self):
        err = round(sum(abs(self.model.predict(self.Xtest) - self.ytest))/len(self.ytest),2)
        print(f'Erreur moyenne : {err}')

    def predict(self,X):
        return self.model.predict(X).map(lambda x:datetime.timedelta(0,x*60))+X[15]
#        X['hour2'] = X.hour.apply(lambda x : x**2)
#        #X['hour3'] = X.hour.apply(lambda x : x**3)
#        X['mer'] = X.day==2
#        X['lun'] = X.day==0
#        X['mar'] = X.day==1
#        X['jeu'] = X.day==3
#        X['ven'] = X.day==4
#plot('day')
        
#        clf = MLPRegressor((20),max_iter = 10000,verbose = True,solver = 'sgd',\
#                           activation = 'relu',learning_rate_init=1e-4,tol = 1e-5,alpha = 1e-4,learning_rate='adaptive')
#        #clf = LinearRegression()
#        clf.fit(Xtrain,ytrain)
#        print(sum(abs(clf.predict(Xtrain) - ytrain))/len(ytrain))
#        print(sum(abs(clf.predict(Xtest) - ytest))/len(ytest))
#        print(clf.score(Xtrain,ytrain))
#        print(clf.score(Xtest,ytest))

if __name__=='__main__':
    c = compute()
    c.get_data()
    c.process()
    c.add_y()
    mod = RegLin()
    mod.train(c.data_ready,c.y)
    mod.test()
