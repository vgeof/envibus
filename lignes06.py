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


class Db():
    def __init__(self,filename = 'data200.csv'):
        self.filename = filename
        self.columns = ['idscrap','time_saved','minutes','theorique']
        try : 
            self.df = pandas.read_csv(self.filename,usecols = self.columns)
        except FileNotFoundError: 
            print('no file found, creating a new one')
            self.df = pandas.DataFrame(columns = self.columns)
            self.df.to_csv(filename)
    def save(self):
        os.system('cp ' + self.filename + ' ' + self.filename + '.bak')
        self.df.to_csv(self.filename)
    def insert(self,data):
        assert(len(data) == len(self.columns))
        self.df = self.df.append(pandas.DataFrame([data],columns = self.columns),sort=False) 
    def get_highest_idscrap(self):
        if len(self.df)==0:return 0
        return self.df['idscrap'].max()

class Scrapper():
    def __init__(self,idarret,db):
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
       temps  = str(soup.find('div',class_ = 'data').find('div')).split('<br/>')
       temps= list(map(lambda x:(x,'*'  in x ),temps))
       temps = list(map(lambda x: (BeautifulSoup(x[0],features = 'lxml'),x[1]),temps))
       temps = list(map(lambda x:(x[0].find('span'),x[1]),temps))
       temps = list(filter(lambda x:x[0] is not None ,temps))
       temps = list(map(lambda x: (x[0].get_text().lower(),x[1]),temps))
       temps = list(map(lambda x :(self.convert_str_to_time(x[0]),x[1]),temps))
       temps = list(filter(lambda x:x[0] is not None ,temps))

       return temps
   
    def insert_in_db(self,temps):
       self.safe_quit = False
       idscrap = self.db.get_highest_idscrap() + 1
       for temp in temps:
           self.db.insert([idscrap,datetime.now(),temp[0],temp[1]])
       self.db.save()
       self.safe_quit = True

    def scrap(self):
       print('request sent')
       try:
           soup = self.requete()
       except Exception as e:
           print(e)
       else : 
           print('success')

           temps = self.extract_from_soup(soup)
          
           print(str(len(temps)) + ' times found' )
           
           return temps
    def launch(self,repos):
        while self.should_continue:
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

    db = Db()
    scrapper = Scrapper(490,db)
    scrapper.launch(15)



