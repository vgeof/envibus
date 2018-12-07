# -*- coding: utf-8 -*-
"""
Created on Tue Dec  4 11:37:20 2018

@author: vgeoffroy
"""
from lignes06 import Scrapper
import pandas
data = []
for i in range (600,800):
    s = Scrapper(i,None)
    try:
        soup = s.requete()
        dd = soup.find('div',class_ = 'here').find_all('span')
    except Exception as e:s
        print(e)
    else:
        if len(dd) ==2:
            try:
                lignes = list(filter(lambda x: x.text.strip()!="",soup.find_all('div',class_ = 'data')))
                lignes = list(map(lambda x: list(x.children)[2].replace(':','').strip(),lignes))
            except Exception as e:
                    print(e)
                    lignes =['NOTFOUND']
            data.append((i,lignes,dd[1].text))
            print((i,lignes,dd[1].text))
    #        with  open('arrets','a') as f:
    #            f.write(str((i,dd[1].text)))

df = pandas.DataFrame(data,columns = ['id','lignes','nom'])

df = df.loc[df['lignes'].map(lambda x:'200'in x)]
df = df.sort_values('nom')