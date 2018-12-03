#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  2 16:01:48 2018

@author: vivien
"""


import pandas
from matplotlib import pyplot as plt
import numpy as np
from shutil import copyfile
import os


class Enricher:
    def __init__(self,deb=None,fin = None):
        
        self.columns = ['idscrap','time_saved','minutes','theorique']
        self.data = pandas.read_csv('data200current.csv',usecols = self.columns,parse_dates=['time_saved'])
        if deb: self.data = self.data.loc[self.data['time_saved']>=deb]
        if fin : self.data = self.data.loc[self.data['time_saved']<=fin]

        
        self.data = self.data.loc[self.data['theorique']==False]
        
    def plot(self,key = 'bus_id'):
        for i in list(set(self.data[key])):
            slic = self.data.loc[self.data[key]==i]
            plt.plot_date(slic['time_saved'],slic['minutes'],'+',label=str(i))

#data['rank'] = data.groupby('idscrap')['minutes'].rank(ascending=True,method = 'first')
    def identifie_bus(self):
        self.data['bus_id'] = pandas.Series(0,index = self.data.index)
        data2 = self.data.copy()
        
        while len(data2)!=0:
            data2['rank'] = data2.groupby('idscrap')['minutes'].rank(ascending=True,method = 'first')
            cur = data2.loc[data2['rank']==1]
            bus_id = self.data['bus_id'].max() + 1
            fin = False
            arrive = False
            while not fin :
    #                plt.clf()
    #                plot()
    #                plt.pause(0.05)
                arrive = cur.iloc[0]['minutes'] < 1
    
                if not arrive and len(cur)>1 :
                    fin = (cur.iloc[1]['time_saved']-cur.iloc[0]['time_saved']).seconds>60*cur.iloc[0]['minutes']\
                    or (cur.iloc[1]['minutes']-cur.iloc[0]['minutes'])>3 and cur.iloc[0]['minutes']<=1
                else: fin = arrive or len(cur)<=1
                self.data.loc[cur.iloc[0].name,'bus_id'] = bus_id
                data2 = data2.drop(cur.index[0])
                cur = cur.drop(cur.index[0])
            if arrive:
                remaining = len(cur)>0
                if remaining:remaining =cur.iloc[0]['minutes']<1 
                while remaining:
                    self.data.loc[cur.iloc[0].name,'bus_id'] = bus_id
                    data2 = data2.drop(cur.index[0])
                    cur = cur.drop(cur.index[0])
                    remaining = len(cur)>0
                    if remaining:remaining =cur.iloc[0]['minutes']<1
                    
            print('bus added ' )


        self.plot()
        plt.show()
        
    def add_arrivees(self):
        
#        def f(x):
#            return x['time_saved'].max()
        arrives = self.data.groupby('bus_id')['minutes'].min()<2
        heures = self.data.groupby('bus_id')['time_saved'].max()
        def summarize(index):
            if arrives.loc[index]: return (index,heures[index])
            else : return (index,None)
#        a = self.data.groupby('bus_id').agg(f)
        arrivees = pandas.DataFrame(list(arrives.index.map(summarize)),columns = ['bus_id','arrivees'])
        self.data = self.data.merge(arrivees,on = 'bus_id')
        arrives = self.data.loc[-self.data['arrivees'].isnull()]
        self.data.loc[arrives.index,'real_minutes']=(arrives['arrivees'] - arrives['time_saved']).map(lambda x:x.seconds/60)
    def save(self):
        ide = len(os.listdir('data_enriched'))
        self.data.to_csv('./data_enriched/data' + str(ide) + '.csv')
        
    def compute_dist(self):
        self.data['distance'] = pandas.Series(0,index = self.data.index)
        self.data['vitesse'] = pandas.Series(0,index = self.data.index)

        data2 = self.data.loc[-self.data['arrivees'].isnull()]
        for name,bus in data2.groupby('bus_id'):
            x0 = 1
            xn = x0
            self.data.loc[bus.index[0],'distance'] = xn

            for i in range(len(bus.index)-1):
                ind = bus.index[i]
                dt = (bus.loc[bus.index[i+1]]['time_saved']-bus.loc[ind]['time_saved'] ).seconds
                trn = bus.loc[ind]['minutes']*60
                #trn = (bus.iloc[0]['minutes']*60-1*ind*dt)
                if trn!=0 :xn1 = xn*(1-dt/trn)
                else :xn1 = 0
                self.data.loc[bus.index[i],'vitesse'] =(xn1-xn)/dt
                xn = xn1
                self.data.loc[bus.index[i+1],'distance'] = xn
            

if __name__=="__main__":
    en = Enricher("2018-11-22",None)
    en.identifie_bus()
#    c = input('continue to saving?(y/n) ')
#    plt.plot_date('time_saved','minutes','+',data = en.data)
#    if c=='y':
#        print('adding arrivees')
#        en.add_arrivees()
#        print('saving')
#        en.save()
#        print('done')
    en.add_arrivees()
    en.compute_dist()
    plt.figure()
    for name,bus in en.data.groupby('bus_id'):
        if not pandas.isna(bus.iloc[0]['arrivees']):
            plt.plot(bus['real_minutes'],bus['vitesse'])