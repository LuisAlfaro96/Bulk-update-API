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

import logging

  
logging.basicConfig(format='%(asctime)s - %(message)s',filename="Population.log",filemode='a', level=logging.INFO)

##############################################################
## Assumes there's a file called creds.json in the form of
##     {
##        "alation_dev": {
##          "api_refresh_secret": "some_text_here"
##          "user_id": "user_id_from_alation"
##        }
##     }
##############################################################

filename = r"/Users/luisenriquealfaroharo/Desktop/Datacatalog/creds.json"

f = open(filename)
j = json.load(f)

refreshToken = j['alation_dev']['api_refresh_secret']
alationUserId = j['alation_dev']['user_id']

##############################################################
## Calls Alation API with Refresh Token to generate an API Token
## Documentation: 
##  https://customerportal.alationdata.com/docs/api_authentication/APIAuthenticationV1/index.html
##############################################################

data = {
    "refresh_token": refreshToken,
    "user_id": alationUserId
}          

try:
  response = requests.post("https://datacatalogdev.cargill.com/integration/v1/createAPIAccessToken/", data=data, verify=False, timeout=5)

  token = response.json()['api_access_token']

except:
  logging.error("Couldn't connect with the server, please check the connection whit it\n")
  logging.error(sys.exit("Closing the script."))

##############################################################
## Calls Alation APIs with Token
## Documentation Samples: 
##  https://datacatalogdev.cglcloud.com/article/138/alation-api-using-postman-to-bulk-load
##  https://datacatalogdev.cglcloud.com/article/309/
##  https://customerportal.alationdata.com/docs/ArticlesAPI/index.html
##  Eventually with an upgrade past Alation version 2020.4 we can turn on the OAS Documentation
##    https://customerportal.alationdata.com/docs/DataSourcesAPI/index.html#description
##    {AlationInstanceURL}/openapi/datasources/
##############################################################

# Get Data Source ID for CDP
response = requests.get("https://datacatalogdev.cargill.com/integration/v1/datasource/",headers={'token': token},verify=False)
results = json.loads(response.text)
#print (type(results))
logging.info('Showing all CDP Data Sources with DS IDs')
for result in results:

    if re.match(r'CDP+',result['title']):
        print(type(result))
        print(result['id'])
        print(result['title'])
  #if result['title'] == 'CDP (Impala)':
    #cdp_ds_id = result['id']
    #chema_list = result['limit_schemas'].split(',')
    if result['title'] == 'CDP (Impala) FULL':# or result['title'] == 'CDP (Impala)':
        cdp_ds_id = result['id']
        limit_schema = result['limit_schemas'].split(',')
        logging.info("Found Datasource --> "+result['title'])
        if len(limit_schema) == 1 and limit_schema[0] == '':
            all_list = result['all_schemas'].split(',')
            exclusion_list = result['exclude_schemas'].split(',')
            schema_list = [schema for schema in all_list if schema not in exclusion_list]
            # print("hi")
        elif len(limit_schema) >= 1:
            schema_list = result['limit_schemas'].split(',')
        #print(len(schema_list))

flag = 0
# For each schema, get the tables
for schema in schema_list:
  
  # Get CDP Table Metadata 
  # https://customerportal.alationdata.com/docs/MetadataGETAPI/index.html
  if schema == "prd_product_lynx": # name of the schema we want to filter
    flag = 1
    #print (schema)
    api_call = "https://datacatalogdev.cargill.com/catalog/table?ds_id=" + str(cdp_ds_id) + "&schema_name=" + str(schema) + "&limit=1000"
    print(api_call)
    response = requests.get(api_call,
    
        headers={
            # 'Content-Type':'application/json',
            'token': token
        },
        verify=False)
    
    results = json.loads(response.text)
    #print(json.dumps(results, indent=4, sort_keys=True))
    
    data = ''

    
    for result in results:#Here are gonna reside all the tables of my schema 
      #i will filter just by one specific table to verify if this approach could work ->    
      
      if result['name'] == 'contract_headers_ordtrdlimeasmtsettp':#result['name']: #name of the table we want to make the changes (we can get rid of this verification and will apply to all al the tables, i hope)
        logging.info("Checking --> Table Name: %s , Schema Name: %s \n" % (result['name'], result['schema_name']))
        #here we will need to add the code to make the verification of the source db comment in the table.
        key = str(result['ds_id'])+'.'+result['schema_name']+'.'+result['name']
        #print(key)
        print(json.dumps(result, indent=4, sort_keys=True))
        if result['db_comment'] == None:
          logging.info("Table: "+result['name']+ " has no Source Comment, moving to the next table\n")
        elif result['db_comment'] == "":
          logging.info("Source Comment empty on table name " + result['name']+ "moving to the next Table...")

        elif result['description'] == "" and result['db_comment'] != "" and result['title'] != "":
          BASE_URL = 'https://datacatalogdev.cargill.com/api/v1/bulk_metadata/custom_fields/'
          data = '{"key":'+'\"'+key+'\"'+', "description":'+'\"'+result['db_comment']+'\"'+'}'
          #print (data)
          response = requests.post(BASE_URL + 'default/table?replace_values=true',data=data.encode('utf-8'),          
            headers={
            # 'Content-Type':'application/json',
            'token': token
            },
            verify=False)
              
          #print (response.text)  
          logging.info("Description Empty, modifying it...\n")
          logging.info("Change done \n")
                  #data_dic['title'] = data_dic['db_comment']
                  #data_dic['db_comment'] = ""
                  #print(json.dumps(data_list[0], indent=4, sort_keys=True))
        elif result['description'] == "" and result['db_comment'] != "" and result['title'] == "":
          BASE_URL = 'https://datacatalogdev.cargill.com/api/v1/bulk_metadata/custom_fields/'
          data = '{"key":'+'\"'+key+'\"'+', "description":'+'\"'+result['db_comment']+'\"'+'}'
          #print (data)
          response = requests.post(BASE_URL + 'default/table?replace_values=true',data=data.encode('utf-8'),          
            headers={
            # 'Content-Type':'application/json',
            'token': token
            },
            verify=False)
              
          #print (response.text)
          logging.info("Description Empty, modifying it...\n")
          logging.info("Change done \n")
          #print(len(result['db_comment']))
          if len(result['db_comment']) < 100:
            BASE_URL = 'https://datacatalogdev.cargill.com/api/v1/bulk_metadata/custom_fields/'
            data = '{"key":'+'\"'+key+'\"'+', "title":'+'\"'+result['db_comment']+'\"'+'}'
            #print (data)
            response = requests.post(BASE_URL + 'default/table?replace_values=true',data=data.encode('utf-8'),          
              headers={
              # 'Content-Type':'application/json',
              'token': token
              },
              verify=False)
                
            #print (response.text)    
            logging.info("Tittle Empty, modifying it...\n")
            logging.info("Change done \n")
                    #data_dic['title'] = data_dic['db_comment']
                    #data_dic['db_comment'] = ""
                    #print(json.dumps(data_list[0], indent=4, sort_keys=True))
          else:
             logging.info("Db_comment too large to populate in title's table ---> " + result['name'])

        elif result['description'] != "" and result['db_comment'] != "" and result['title'] == "":
          if len(result['db_comment']) < 100:
            BASE_URL = 'https://datacatalogdev.cargill.com/api/v1/bulk_metadata/custom_fields/'
            data = '{"key":'+'\"'+key+'\"'+', "title":'+'\"'+result['db_comment']+'\"'+'}'
            #print (data)
            response = requests.post(BASE_URL + 'default/table?replace_values=true',data=data.encode('utf-8'),          
              headers={
              # 'Content-Type':'application/json',
              'token': token
              },
              verify=False)
                
            #print (response.text)    
            logging.info("Tittle Empty, modifying it...\n")
            logging.info("Change done \n")
                    #data_dic['title'] = data_dic['db_comment']
                    #data_dic['db_comment'] = ""
                    #print(json.dumps(data_list[0], indent=4, sort_keys=True))
          else:
             logging.info("Db_comment too large to populate in title in table " + result['name']+"\n")

        else:   
          logging.info ("All good with the table:"+result['name']+ "nothing to modify\n")
        ########################################################### adding new code #######################################################
                # here its something tricky, in my results(the one above) there is not specific way to call the column's Table by id, for example https://datacatalogdev.cargill.com/catalog/column/?table_id=130767
        #so what am i doing its to call all the columns where the name of the table matched my table_name value
        #you can chechk what could be the result with the URL https://datacatalogdev.cargill.com/catalog/column/?table_name=prd_product_lynx.contract_headers_ordtrdlimeasmtsettp
        #for that particular example, you can see that there are some results that reside in another schema(it has other schema ID), since the schema name and table name reside in both enviroments (see the image in the card #1893 to uderstand or just search for prd_product_lynx.contract_headers_ordtrdlimeasmtsettp in the dev datacatalogdev)
        api_call = "https://datacatalogdev.cargill.com/catalog/column/?table_name=" + schema +'.'+ result['name'] 
        #api_call = https://datacatalogdev.cargill.com/catalog/column/?table_name=prd_product_lynx.contract_headers_ordtrdlimeasmtsettp
        print(api_call)
        response = requests.get(api_call,
      
          headers={
              # 'Content-Type':'application/json',
              'token': token
          },
          verify=False)
      
        results = json.loads(response.text)
        #print(results)
        for result in results: # here we extract all the columns and make the corresponding evaluation in the description value  for each table.
          logging.info("Checking --> Column Name: %s \n" % (result['name']))
          key = str(result['ds_id'])+'.'+result['table_name']+'.'+result['name']
          print(key)
        
          if result['db_comment'] == None:
            logging.info("Column: "+result['name']+ " has no Source Comment, moving to the next column\n")
          elif result['db_comment'] == "":
            logging.info("Source Comment empty on column name " + result['name']+ "moving to the next Column...")

          elif result['description'] == "" and result['db_comment'] != "" and result['title'] != "":
            BASE_URL = 'https://datacatalogdev.cargill.com/api/v1/bulk_metadata/custom_fields/'
            data = '{"key":'+'\"'+key+'\"'+', "description":'+'\"'+result['db_comment']+'\"'+'}'
            print (data)
            response = requests.post(BASE_URL + 'default/attribute?replace_values=true',data=data.encode('utf-8'),          
              headers={
              # 'Content-Type':'application/json',
              'token': token
              },
              verify=False)
            logging.info(response.status_code)     
            logging.info("Description Empty, modifying it...\n")
            logging.info("Change done \n")
                    #data_dic['title'] = data_dic['db_comment']
                    #data_dic['db_comment'] = ""
                    #print(json.dumps(data_list[0], indent=4, sort_keys=True))
          elif result['description'] == "" and result['db_comment'] != "" and result['title'] == "":
            BASE_URL = 'https://datacatalogdev.cargill.com/api/v1/bulk_metadata/custom_fields/'
            data = '{"key":'+'\"'+key+'\"'+', "description":'+'\"'+result['db_comment']+'\"'+'}'
            #print (data)
            response = requests.post(BASE_URL + 'default/attribute?replace_values=true',data=data.encode('utf-8'),          
              headers={
              # 'Content-Type':'application/json',
              'token': token
              },
              verify=False)
            logging.info(response.status_code)               
            logging.info("Description Empty, modifying it...\n")
            logging.info("Change done \n")
            if len(result['db_comment']) < 100:
              BASE_URL = 'https://datacatalogdev.cargill.com/api/v1/bulk_metadata/custom_fields/'
              data = '{"key":'+'\"'+key+'\"'+', "title":'+'\"'+result['db_comment']+'\"'+'}'
              #print (data)
              response = requests.post(BASE_URL + 'default/attribute?replace_values=true',data=data.encode('utf-8'),          
                headers={
                # 'Content-Type':'application/json',
                'token': token
                },
                verify=False)
              logging.info(response.status_code)   
              logging.info("Title Empty, modifying it...\n")
              logging.info("Change done \n")
                      #data_dic['title'] = data_dic['db_comment']
                      #data_dic['db_comment'] = ""
                      #print(json.dumps(data_list[0], indent=4, sort_keys=True))
            else:
               logging.info("Db_comment too large to populate in title in column " + result['name'])

          elif result['description'] != "" and result['db_comment'] != "" and result['title'] == "":
            if len(result['db_comment']) < 100:
              BASE_URL = 'https://datacatalogdev.cargill.com/api/v1/bulk_metadata/custom_fields/'
              data = '{"key":'+'\"'+key+'\"'+', "title":'+'\"'+result['db_comment']+'\"'+'}'
              #print (data)
              response = requests.post(BASE_URL + 'default/attribute?replace_values=true',data=data.encode('utf-8'),          
                headers={
                # 'Content-Type':'application/json',
                'token': token
                },
                verify=False)
              logging.info(response.status_code)    
              logging.info("Tittle Empty, modifying it...\n")
              logging.info("Change done \n")
                      #data_dic['title'] = data_dic['db_comment']
                      #data_dic['db_comment'] = ""
                      #print(json.dumps(data_list[0], indent=4, sort_keys=True))
            else:
              logging.info("Db_comment too large to populate in title in column " + result['name'])
          else:
            logging.info ("Nothing to do on column"+ result['name'] +" in table " + result['table_name'])  
       ################################################# end of new code #######################################################
      else:
        logging.info("No table found on Schema: "+schema+"\n")
if flag == 0:
  logging.info("-> Not schema found")
else:
  logging.info("All the corresponding validation were made")

