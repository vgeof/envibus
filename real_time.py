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
reload( logging)

logging.basicConfig(level=logging.INFO)

subprocess.call(['./copy_recent.sh'])
en = Enricher('recent.db',490)
c = compute()
c.get_data()
c.process()
c.add_y()
mod = RegLin()
mod.train(c.data_ready,c.y)

subprocess.call(['./copy_recent.sh'])
en.enrich_fast('suppress')
new_data = compute(490,'recent.db',minutes_min = 0,nb_data_min = 0)
new_data.get_data()
new_data.process()
pred = mod.predict(new_data.data_ready).to_frame()
en.cursor.execute("""SELECT BUS.id_bus,minutes,max(time_saved) as 'last [timestamp]' from SCRAPPED INNER JOIN BUS ON BUS.id = SCRAPPED.id WHERE BUS.id_bus <>-1 GROUP BY BUS.id_bus""")
raw = en.cursor.fetchall()
df = pandas.DataFrame(list(map(lambda x:datetime.timedelta(0,x[1]*60)+x[2],raw)),columns = ['last'],index  = list(map(lambda x:x[0],raw)))
pred = pandas.concat([pred,df],join = 'inner', axis = 1)
time.sleep(5)
