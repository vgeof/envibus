#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  7 17:41:09 2018

@author: vivien
"""

from os import listdir
from os.path import isfile
import re
import pandas
from matplotlib import pyplot as plt
from enrich06 import Enricher
os.chdir('data_raw')
arrets = [490,224, 238, 208,207,236,235,218, 233,227,229,241,230]
def assemble():
    files = [f for f in listdir() if isfile(f) and re.match('data200_\d{1,3}.csv$',f)]
    columns = ['idscrap','time_saved','minutes','theorique']
    df = pandas.DataFrame(columns = ['idarret']+columns)
    for f in files:
        cur = pandas.read_csv(f,usecols = columns,parse_dates = ['time_saved'])
        idarret = re.match('data200_(\d{1,3}).csv$',f).groups()[0]
        cur['idarret'] = pandas.Series(idarret,index = cur.index)
        cur = cur[['idarret']+columns]
        df = df.append(cur)
    df = df.reset_index()
#    df = df.loc[df['theorique']==False]
    
    df['minutes'] = df['minutes'].astype('int')
#    df['rank'] = df.groupby('idscrap')['minutes'].rank(ascending=True,method = 'first')
#    df = df.loc[df['time_saved']>'2018-12-05 17:00:00']
##    df = df.loc[df['time_saved']<'2018-12-05 18:00:00']
#    df.save(filename)
#
#for arret,data in df.groupby('idarret'):
#    if arret in ('490','208','235'):
#        plt.plot_date(data['time_saved'],data['minutes'],'+')
##        
#
#files = [f for f in listdir() if isfile(f) and re.match('data200_\d{1,3}.csv$',f)]
#   
#for f in files:
#     idarret = re.match('data200_(\d{1,3}).csv$',f).groups()[0]
#     if int(idarret)>=238:
#         en = Enricher(f,idarret,"2018-11-22",None,ignored = True)
#         en.identifie_bus2()

        
