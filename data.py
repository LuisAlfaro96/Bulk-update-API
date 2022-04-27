import io
import os
import json
import requests
import re
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import pandas as pd
import sys
from datetime import datetime
from pandas.io.json import json_normalize
import yaml
from yaml.loader import SafeLoader

import logging


logging.basicConfig(format='%(asctime)s - %(message)s',filename="Population.log",filemode='a', level=logging.INFO)

# Open the file and load the file
with open('domains.yml') as f:
    data = yaml.load(f, Loader=SafeLoader)
    print(len(data['domains']))
    all_domains = data['domains']


    for domain in all_domains:
        print(domain)
        api_call = "https://api.domaintools.com/v1/" + str(domain) + "/whois/"
        print(api_call)
        response = requests.get(api_call)
        result = json.loads(response.text)
    #print(result)
            
        value = result['response']['registration']
        value.pop('registrar')
        value.pop('statuses')

        
        print(value)
        print(type(value))
        name_file = '/home/luis/Desktop/Pfizer/domain_data/' + domain + "_data.json"
        name_file2 = '/home/luis/Desktop/Pfizer/domain_data/updated_info.json'
        if os.path.isfile(name_file):
            print("The file already exists")
    
            listObj = []
            with open(name_file) as fp:
                listObj = json.load(fp)
                #make the comparation with the last data and the new entry 
                data_to_compare= listObj[-1]
                print (data_to_compare)
                if  data_to_compare == value:
                    print("son iguales loggin")
                else:
                    for clave in value:
                        
                        print(value[clave])
            
                        print(data_to_compare[clave])

                        if value[clave] != data_to_compare[clave]: #quedamos en que aun no sacamos bien los valores que tenemos que enviar al archivo update_info
                            new_dic = {}
                            new_dic["Domain"] = domain
                            new_dic["New entry_"+clave] = value[clave] #porque se guarda 2 veces el ultimo valor
                            new_dic["Old data_"+clave] = data_to_compare[clave]
                            data_updated = new_dic
                            print("son diferentes loggin")
                            print(data_updated)
                            if os.path.isfile(name_file2):
                                print("The file already exists")
                                listObj2 = []
                                with open(name_file2) as fp:
                                    listObj2 = json.load(fp)
                                    listObj2.append(data_updated)
                                    with open(name_file2, 'w') as json_file:
                                        json.dump(listObj2, json_file,indent=4,separators=(',',': '))
                            else:

                                with open(name_file2,'w') as outfile:
                                    data_updated['Domain'] = domain
                                    array = []
                                    array.append(data_updated)
                                    #array.append({'Domain': domain})
                                    json.dump(array, outfile)
                        else:
                            print("doing nothing")  
                                    
                

                listObj.append(value)
                print("llegando")
            with open(name_file, 'w') as json_file:
                json.dump(listObj, json_file,indent=4,separators=(',',': '))


        else:
            with open(name_file,'w') as outfile:
                array = []
                array.append(value)
                json.dump(array, outfile)





