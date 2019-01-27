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


subprocess.call(['./copy_recent.sh'])
en = Enricher('recent.db',490)
c = compute()
c.get_data()
c.process()
c.add_y()
mod = RegLin()
mod.train(c.data_ready,c.y)

while True : 
    subprocess.call(['./copy_recent.sh'])
    time.sleep(1)
    en.identifie_bus2('suppress')
    en.plot()
    new_data = compute(490,'recent.db',minutes_min = 0,nb_data_min = 0)
    new_data.get_data()
    new_data.process()
    mod.predict(new_data.data_ready)
    time.sleep(5)

