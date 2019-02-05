#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 20 20:03:59 2019

@author: vivien
"""

from enrich06 import Enricher
from basic_analys import compute,RegLin
import subprocess
import time
import pandas
import datetime
from imp import reload
import logging
reload( logging)
from bottle import route, run, template


logging.basicConfig(level=logging.INFO)

subprocess.call(['./copy_recent.sh'])
en = Enricher('recent.db',490)
c = compute()
c.get_data()
c.process('reglin')
c.add_y()
mod = RegLin()
mod.train(c.data_ready,c.y)

@route('/pred/')
def index():

    subprocess.call(['./copy_recent.sh'])
    en.enrich_fast('suppress')
    new_data = compute(490,'recent.db',minutes_min = 16,nb_data_min = 0)
    new_data.get_data()
    new_data.process('reglin')
    pred = mod.predict(new_data.data_ready)
    pred.name = "prediction"
    en.cursor.execute("""SELECT BUS.id_bus,max(time_saved) as 'last [timestamp]',minutes from SCRAPPED INNER JOIN BUS ON BUS.id = SCRAPPED.id WHERE BUS.id_bus <>-1 GROUP BY BUS.id_bus""")
    raw = en.cursor.fetchall()
    df = pandas.DataFrame(list(map(lambda x:(x[1],x[2]),raw)),columns = ['last','last_minutes'],index  = list(map(lambda x:x[0],raw)))
    pred2 = pandas.concat([pred.to_frame(),df],join = 'inner', axis = 1)
    return template('make_table', rows=pred2.values,desc = pred2.columns)
#time.sleep(5)

#    return template('<b>Hello {{name}}</b>!', name=name)

run(host='localhost', port=8080)
