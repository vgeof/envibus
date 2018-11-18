import requests
from bs4 import BeautifulSoup
import re


map_arrets = {
        'LHO1' : '490',
        'LHO2' : '491',
        'LHO12' : '490%2C491'
        }
def search(arret):
    arret = map_arrets[arret]
    r=requests.post('http://www.envibus.fr/?dklik_boutique%5Baction%5D=select_arret&id_ligne=all&id_arret='+arret+'&from_ligne=false')
    
    soup = BeautifulSoup(r.text)
    
    res = soup.find_all('div',class_ = 'horraire_passage_container')
    res2 = soup.find_all('div',class_ = 'nom_ligne')
    
    def extract_code_line(element):
        return element.span.get_text()
    
    res2 = list(map(extract_code_line,res2))
    def extract_passage(element):
        passages = element.find_all('p')
        ret = []
        for passage in passages:
            text = passage.get_text().lower()
            time = re.search('dans(.*)minutes',text)
            if time:
                time = time.groups(0)[0].strip()
                direction = re.search('direction(.*)',text).groups(0)[0].strip()
                ret.append({'temps':time,'direction':direction})
        return ret
    
    
    res = list(map(extract_passage,res))
    final = {}
    for i in range(len(res2)):
        final[res2[i]] = res[i]
    return final
