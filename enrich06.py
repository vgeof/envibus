#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  2 16:01:48 2018

@author: vivien
"""


import pandas
from matplotlib import pyplot as plt
import functools
import operator
import datetime
import sys
import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
def avg_datetime(series):
    dt_min = series.min()
    deltas = [x-dt_min for x in series]
    return dt_min + functools.reduce(operator.add, deltas) / len(deltas)

class Enricher:
    def __init__(self,filename,arret,deb=None,fin = None,save = True):
        

        self.arret = arret
        self.conn = sqlite3.connect(filename,detect_types = sqlite3.PARSE_COLNAMES)
        self.cursor = self.conn.cursor() 
        if fin is None: self.fin='2999-10-10'
        else: self.fin=fin
        if deb is None: self.deb='1999-10-10'
        else:self.deb=deb
    
    def enrich_fast(self,mode):
        self.cursor.execute("SELECT time_saved as 'time_saved [timestamp]' FROM SCRAPPED WHERE id_arret = ? AND id NOT IN(SELECT id FROM BUS)\
                                AND time_saved >? AND time_saved <?  AND theorique =0 ORDER BY time_saved"\
                            ,(self.arret,self.deb,self.fin))
        times = list(map(lambda x :x[0],self.cursor.fetchall()))
        front = [self.deb]
        exdeb = self.deb
        exfin=self.fin
        for i in range(len(times)-1):
            if (times[i+1]-times[i]).seconds>4*3600:
                front.append(times[i] +  (times[i+1]-times[i])/2)
        front.append(self.fin)
        for i in range(len(front)-1):
            self.deb = front[i]
            self.fin = front[i+1]
            logging.info('range : '+str(self.deb) + '->' + str(self.fin))
            self.identifie_bus2(mode)
        self.deb = exdeb
        self.fin = exfin
                
    def plot(self,key = 'id_bus'):
        self.cursor.execute("SELECT time_saved as  'time_saved [timestamp]',minutes,? FROM SCRAPPED LEFT OUTER JOIN BUS on BUS.id =\
                            SCRAPPED.id WHERE time_saved >? AND time_saved <? AND id_arret=? AND theorique = 0"\
                            ,(key,self.deb,self.fin,self.arret))
        data = pandas.DataFrame(self.cursor.fetchall(),\
                                    columns = list(map(lambda x:x[0],self.cursor.description))).fillna(-1)
        for i in list(set(data[key])):
            slic = data.loc[data[key]==i]
            plt.plot_date(slic['time_saved'],slic['minutes'],'+',label=str(i))
            
#    def drop_ignored(self):
#        self.conn.execute("SELECT * FROM IGNORED WHERE id_arret=?",self.arret)
#        for row in self.ignoreds.loc[self.ignoreds.arret == str(self.arret) ].iterrows():
#            row = row[1]
#            query= "idscrap==@row.idscrap & time_saved == @row.time_saved & minutes == @row.minutes"  
#            founds = self.data.query(query)
#            if len(founds)==1:
#                self.data.drop(founds.iloc[0].name,inplace = True)

    
    def add_ignored(self,id_):
            self.cursor.execute("INSERT INTO BUS(id,id_bus) VALUES(?,-1)",(int(id_),))
            self.conn.commit()
#data['rank'] = data.groupby('idscrap')['minutes'].rank(ascending=True,method = 'first')
    
    def add_bus(self,bus_ids):
        self.cursor.execute("SELECT MAX(id_bus) FROM BUS")
        bus_id = self.cursor.fetchone()[0]
        if bus_id is None: bus_id=1
        else: bus_id = bus_id+1
        elts = list(map(lambda x : (int(x),int(bus_id)),bus_ids))
        self.cursor.executemany("INSERT INTO BUS(id,id_bus) VALUES(?,?)",elts)
        self.conn.commit()
        
    def identifie_bus2(self,mode = 'manual'):
        assert(mode in ('manual','suppress','continue'))  
        data = ('pipeau')
        while len(data)!=0:
            self.cursor.execute(""" WITH TO_DO AS(
                    SELECT * FROM SCRAPPED WHERE id_arret = ? AND id NOT IN(SELECT id FROM BUS)
                                AND time_saved >?
                                AND time_saved <?
                                AND NOT theorique)
                                SELECT id,
                                RANK() OVER(PARTITION BY t1.id_scrap ORDER BY minutes) rank,
                                minutes,
                                t1.id_scrap AS id_scrap,
                                time_saved as 'time_saved [timestamp]',
                                t3.mean as minutes_mean,
                                t2.count as nb_scrap
                                FROM TO_DO t1
                                INNER JOIN (SELECT id_scrap,COUNT(id) as count FROM TO_DO GROUP BY id_scrap) t2 
                                ON t1.id_scrap = t2.id_scrap
                                INNER JOIN (SELECT id_scrap,AVG(minutes) as mean FROM TO_DO GROUP BY id_scrap) t3
                                WHERE t3.id_scrap = t1.id_scrap
                                ORDER BY t1.time_saved""",(self.arret,self.deb,self.fin))
            data = pandas.DataFrame(self.cursor.fetchall(),\
                                    columns = list(map(lambda x:x[0],self.cursor.description)))
            if len(data) ==0: break
            firsts = data.loc[data['rank']==1].groupby('id_scrap',as_index=False).first()
#            print(firsts.iloc[1])
            
            fin = False
            retry = False
            bus_ids = []
            while not fin and not retry:
    #                plt.clf()
    #                plot()
    #                plt.pause(0.05)
                current = firsts.iloc[0]
                bug = False
                if len(firsts)<=1:
                    fin = True
                    logging.debug('end of firsts')
                else :
                    nex = firsts.iloc[1]
                    if  (nex['time_saved']-current['time_saved']).seconds>60*(current['minutes']+1):
                        fin = True
                        logging.debug('tracking lost')
                    elif (-nex['minutes'] + current['minutes'])*60/(nex['time_saved'] - current.time_saved).seconds>7:
                        logging.debug('WARNING : Violent variation of speed detected')
                        logging.debug('speed : '   + str((-nex['minutes'] + current['minutes'])\
                              *60/(nex['time_saved'] - current.time_saved).seconds))
                        bug = True
                    if nex['nb_scrap'] <current['nb_scrap']:
                        fin = True
                        logging.debug('nb_scrap decreases')
                    
                    elif nex['nb_scrap'] ==current['nb_scrap'] and nex['minutes_mean'] >=current['minutes_mean']+5:
                        logging.debug('nb_bus steady, but a new bus has been probbly added')
                        fin = True
                    elif nex['nb_scrap'] ==current['nb_scrap'] and bug:
                        logging.debug('No bug since nb_scrap is steady')
                        bug = False
                if fin : logging.info('bus added :' + str(current.time_saved))

                if bug : 
                    others = data.loc[data['id_scrap']==nex['id_scrap']].copy()
#                    print(others)
                    if len(others)==1:
                        logging.debug('no bug since bus is the last one')
                        ans = 'n'
                    else:
                        others['rank']=others.groupby('id_scrap')['minutes'].rank(ascending=True,method = 'first')
#                        print(others)
                        bus_up = others.loc[others['rank']==2].iloc[0]
                        if abs(bus_up['minutes']-current['minutes'])<abs(nex['minutes']-current['minutes']):
                            logging.debug('Real bug detected since a Bus inserted !!! ')
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
                        else: ans = 'n'
                                
                    
                    if ans =='y':
                        self.add_ignored(nex.id)
                        retry = True
                    elif len(ans) ==2:
                        if ans[1]=='y':
                            nb = int(ans[0])
                            for i in range(nb):
                                nex = firsts.iloc[i+1]
                                self.add_ignored(nex.id)
                                retry = True
                    elif ans =='exit' :  sys.exit(0)
                    else : pass
                
                bus_ids.append(current.id)            
                firsts = firsts.drop(current.name)
            if not retry: self.add_bus(bus_ids)
                        
        
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
    en = Enricher('data.db',490,"2018-12-05")
#    en.identifie_bus2('continue')
#
    en.enrich_fast('suppress')
#    en.add_arrivees()
#    en.compute_dist()
#    plt.figure()
#    for name,bus in en.data.groupby('bus_id'):
#        if not pandas.isna(bus.iloc[0]['arrivees']):
#            plt.plot(bus['real_minutes'],bus['vitesse'])
##            
            
            

#    plt.plot_date(en.data['time_saved'],en.data['nb_scrap'])
#    plt.plot_date(en.data['time_saved'],en.data['minutes_mean'])
