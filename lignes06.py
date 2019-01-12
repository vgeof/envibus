import requests
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import time,date
import time
import re
import pandas
import os
import signal
import sys
from shutil import copyfile
import sys
import sqlite3


class Db():
    def __init__(self,filename = 'data.db'):
        self.conn = sqlite3.connect(filename)
        self.cursor = self.conn.cursor()
        self.columns = ['id_arret','id_scrap','time_saved','minutes','theorique']
    def insert(self,data):
        assert(len(data) == len(self.columns))
        self.cursor.execute("INSERT INTO SCRAPPED("+','.join(self.columns)+") VALUES(?,?,?,?,?)",data)
        self.conn.commit()
    def get_highest_idscrap(self,arret):
        t = (arret,)
        self.cursor.execute("SELECT MAX(id_scrap) FROM SCRAPPED WHERE id_arret=?",t)
        return self.cursor.fetchone()[0]

class Scrapper():
    def __init__(self,idarret,db,max_time = 3600):
        self.start = time.time()
        self.max_time = max_time
        self.idarret = idarret
        self.db = db
        self.should_continue = True
        signal.signal(signal.SIGINT, self.signal_handler)
        self.safe_quit = True

    def requete(self):
        url = 'http://cg06.tsi.cityway.fr/qrcode' 
        r = requests.get(url,params = {'id': str(self.idarret)})
        if r.status_code !=200:
            raise Exception('Error : site returned status ' + r.status)
        else:
            return BeautifulSoup(r.text,features = "lxml")

    def convert_str_to_time(self,texte):
        if '*' in texte:return None
        
        else:

            ret = re.search('.+h.+',texte )
            if ret:
                ret = datetime.strptime(ret.group(),'%Hh%M')
                ret = ret.replace(year = date.today().year,day = date.today().day,month = date.today().month)
                ret = (ret - datetime.now()).seconds//60
            else:
                try:
                    ret = int(texte)
                except:
                        
                    print('Warning : unable to convert ' + texte + ' into number')
                    ret = None
            return ret
        
    def extract_from_soup(self,soup):
       def taff(string):
           if 'approche' in string : return 0
           else:
               soup = BeautifulSoup(string,features = 'lxml')
               elt = soup.find('span')
               if elt:
                   return self.convert_str_to_time(elt.get_text().lower())
       temps  = list(filter(lambda x:'200' in str(x),soup.find_all('div',class_ = 'data')))
       if len(temps)!=1:print('WARNING : Several lines are found, only first is parsed')
       temps = temps[0]
       temps = str(temps.find('div')).split('<br/>')
       temps= list(map(lambda x:(x,'*'  in x ),temps))
       temps = list(map(lambda x: (taff(x[0]),x[1]) ,temps))
#       temps = list(map(lambda x: (BeautifulSoup(x[0],features = 'lxml'),x[1]),temps))
#       temps = list(map(lambda x:(x[0].find('span'),x[1]),temps))
##       temps = list(filter(lambda x:x[0] is not None ,temps))
#       temps = list(map(lambda x: (x[0].get_text().lower(),x[1]),temps))
#       temps = list(map(lambda x :(self.convert_str_to_time(x[0]),x[1]),temps))
       temps = list(filter(lambda x:x[0] is not None ,temps))

       return temps
   
    def insert_in_db(self,temps):
       self.safe_quit = False
       idscrap = self.db.get_highest_idscrap(self.idarret) + 1
       for temp in temps:
           self.db.insert([self.idarret,idscrap,datetime.now(),temp[0],temp[1]])
       self.safe_quit = True

    def scrap(self):
       print('request sent for stop : '+ str(self.idarret))
       try:
           soup = self.requete()
       except Exception as e:
           raise(e)
       else : 
           print('success')

           temps = self.extract_from_soup(soup)
          
           print(str(len(temps)) + ' times found' )
           
           return temps
    def launch(self,repos):
        while self.should_continue and time.time()-self.start <self.max_time:
            try:
                temps = self.scrap()
            except Exception as e:
                print(e)
            else:   
                self.insert_in_db(temps)
            if self.should_continue:
                print('waiting ' + str(repos) + ' seconds...')
                time.sleep(repos)
            
    def signal_handler(self,sig, frame):
        print('You pressed Ctrl+C! , exiting...')
        if self.safe_quit: sys.exit(0)
        self.should_continue = False        

if __name__ =='__main__':
#    arret = sys.argv[1]
    arret = 490
    print(arret)
#    sys.stdout = open('log' + str(arret), 'w')
    db = Db()
    scrapper = Scrapper(arret,db,4*3600)
    scrapper.launch(1)
