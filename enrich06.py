#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  2 16:01:48 2018

@author: vivien
"""


import pandas
from matplotlib import pyplot as plt
import numpy as np
import functools
import operator
from shutil import copyfile
import os
import datetime
import sys


def avg_datetime(series):
    dt_min = series.min()
    deltas = [x-dt_min for x in series]
    return dt_min + functools.reduce(operator.add, deltas) / len(deltas)

class Enricher:
    def __init__(self,filename,arret,deb=None,fin = None,ignored = True):
        
        self.columns = ['idscrap','time_saved','minutes','theorique']
        self.data = pandas.read_csv(filename,usecols = self.columns,parse_dates=['time_saved'])
        self.arret = arret
        if deb: self.data = self.data.loc[self.data['time_saved']>=deb]
        if fin : self.data = self.data.loc[self.data['time_saved']<=fin]
        self.ignored = ignored
        if ignored : self.ignoreds = pandas.read_csv('ignored.csv',usecols = ['arret','idscrap','time_saved','minutes'],dtype = str)
        
        self.data = self.data.loc[self.data['theorique']==False]
        self.drop_ignored()
    def plot(self,key = 'bus_id'):
        for i in list(set(self.data[key])):
            slic = self.data.loc[self.data[key]==i]
            plt.plot_date(slic['time_saved'],slic['minutes'],'+',label=str(i))
            
    def drop_ignored(self):
        for row in self.ignoreds.loc[self.ignoreds.arret == str(self.arret) ].iterrows():
            row = row[1]
            query= "idscrap==@row.idscrap & time_saved == @row.time_saved & minutes == @row.minutes"  
            founds = self.data.query(query)
            if len(founds)==1:
                self.data.drop(founds.iloc[0].name,inplace = True)
    def search_ignored(self,arret,idscrap,time_saved,minutes):
        if not self.ignored : return
        query = "arret == @arret & idscrap==@idscrap & time_saved == @time_saved & minutes == @minutes" 
        return  self.ignoreds.query(query)
    
    def add_ignored(self,arret,idscrap,time_saved,minutes):
        arret = str(arret)
        idscrap = str(idscrap)
        time_saved = str(time_saved)
        minutes = str(minutes)
        if not self.ignored : return
        founds = self.search_ignored(arret,idscrap,time_saved,minutes)
        if len(founds) > 0 : print('already in database')
        else : 
            self.ignoreds = self.ignoreds.append(pandas.DataFrame([(arret,idscrap,time_saved,minutes)],columns = self.ignoreds.columns))
            self.ignoreds.to_csv('ignored.csv')    
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
                    or (cur.iloc[1]['minutes']-cur.iloc[0]['minutes'])>3 and cur.iloc[0]['minutes']<=1\
                    or (cur.iloc[1]['minutes']-cur.iloc[0]['minutes'])>7
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
        
    def add_idscrap_count(self):
            self.data = self.data.merge(pandas.DataFrame(self.data.groupby('idscrap')['time_saved'].count())\
                            .rename(columns = {'time_saved':'nb_scrap'}),on = 'idscrap')\
                            .set_index(self.data.index)
            self.data = self.data.merge(pandas.DataFrame(self.data.groupby('idscrap')['minutes'].mean())\
                            .rename(columns = {'minutes':'minutes_mean'}),on = 'idscrap')\
                            .set_index(self.data.index)

        
    def identifie_bus2(self,mode = 'manual'):
        assert(mode in ('manual','suppress','continue'))
        self.data['bus_id'] = pandas.Series(0,index = self.data.index)
        data = self.data.copy()
        
        
        while len(data)!=0:
            def compute(data2):
                data2['rank'] = data2.groupby('idscrap')['minutes'].rank(ascending=True,method = 'first')
                try : 
                    data2.drop(['nb_scrap','minutes_mean'],1,inplace = True)
                except KeyError:pass
                index = data2.index
                data2 = data2.merge(pandas.DataFrame(data2.groupby('idscrap')['minutes'].mean())\
                                .rename(columns = {'minutes':'minutes_mean'}),on = 'idscrap')
                data2 = data2.merge(pandas.DataFrame(data2.groupby('idscrap')['time_saved'].count())\
                                .rename(columns = {'time_saved':'nb_scrap'}),on = 'idscrap')\
                                    .set_index(index)
                firsts = data2.loc[data2['rank']==1]
                return firsts,data2
            firsts,data = compute(data)
            bus_id = self.data['bus_id'].max() + 1
            fin = False
            
            while not fin :
    #                plt.clf()
    #                plot()
    #                plt.pause(0.05)
                current = firsts.iloc[0]
                bug = False
                if len(firsts)<=1:
                    fin = True
                    print('end of firsts')
                else :
                    nex = firsts.iloc[1]
                    if  (nex['time_saved']-current['time_saved']).seconds>60*(current['minutes']+1):
                        fin = True
                        print('tracking lost')
                    elif (-nex['minutes'] + current['minutes'])*60/(nex['time_saved'] - current.time_saved).seconds>10:
                        print('WARNING : Violent increase of speed detected')
                        print('speed : '   + str((-nex['minutes'] + current['minutes'])\
                              *60/(nex['time_saved'] - current.time_saved).seconds))
                        bug = True
                    elif nex['nb_scrap'] <current['nb_scrap']:
                        fin = True
                        print('nb_scrap decreases')
                    
                    elif nex['nb_scrap'] ==current['nb_scrap'] and nex['minutes_mean'] >=current['minutes_mean']+5:
                        print('nb_bus steady, but a new bus has been probbly added')
                        fin = True
                if fin : print('bus added :' + str(current.time_saved))

                if bug : 
                    if mode=='manual':
                        plt.clf()
                        self.plot()                    
                        plt.plot(nex['time_saved'],nex['minutes'],'o')
                        plt.xlim ((nex['time_saved'] + datetime.timedelta(0,-150),nex['time_saved']+ datetime.timedelta(0,150))) 
                        plt.draw()
                        plt.show(block = False)
                        plt.pause(0.05)
                        ans = input('deleting point ? (y:yes/ n: no and continue/ exit : exit) > ')
                        plt.close()
                    elif mode=='suppress' : ans = 'y'
                    elif mode =='continue' : ans = 'n'
                    if ans =='y':
                        self.data.drop(nex.name,inplace = True)
                        self.add_ignored(self.arret,nex.idscrap,nex.time_saved,nex.minutes)
                        self.identifie_bus2()
                        return  
                    elif len(ans) ==2:
                        if ans[1]=='y':
                            nb = int(ans[0])
                            for i in range(nb):
                                nex = firsts.iloc[i+1]
                                self.data.drop(nex.name,inplace = True)
                                self.add_ignored(self.arret,nex.idscrap,nex.time_saved,nex.minutes)
                            self.identifie_bus2()
                            return  
                    elif ans =='exit' :  sys.exit(0)
                    else : pass
                
                            
                self.data.loc[current.name,'bus_id'] = bus_id
                data = data.drop(current.name)
                firsts = firsts.drop(current.name)
                        
            
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
    en = Enricher('data_raw/data200_490.csv',490,"2018-11-23",None,True)
    en.identifie_bus2('continue')
#
#    print('adding arrivees')
#    en.add_arrivees()
#    print('saving')
#    en.save()
#    print('done')
#    en.add_arrivees()
#    en.compute_dist()
#    plt.figure()
#    for name,bus in en.data.groupby('bus_id'):
#        if not pandas.isna(bus.iloc[0]['arrivees']):
#            plt.plot(bus['real_minutes'],bus['vitesse'])
#            
            
            

#    plt.plot_date(en.data['time_saved'],en.data['nb_scrap'])
#    plt.plot_date(en.data['time_saved'],en.data['minutes_mean'])