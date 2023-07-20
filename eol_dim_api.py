# -*- coding: utf-8 -*-
"""
Created on Sun May  7 15:16:58 2023

@author: alelima
"""

# importing the requests library
import requests
import pandas as pd
#import numpy as np
import json
import psycopg2
#from psycopg2 import Error
from datetime import datetime

# cd "C:\Users\alelima\OneDrive - Cisco\PUC\Classroom\2023-1-PROJ\CSV input\PUC modified"
 
#pid_list=inventory_df['Product Id'].unique()
#inventory_df=pd.DataFrame()
#inventory_df=pd.read_csv(r'C:\Users\alelima\OneDrive - Cisco\PUC\Classroom\2023-1-PROJ\CSV input\PUC modified\DatabaseTemp.csv',sep=';') 
inventory_df=pd.read_csv(r'C:\Users\alelima\OneDrive - Cisco\PUC\Classroom\2023-1-PROJ\CSV input\PUC modified\DatabaseTemp.csv',sep=';', converters={'nk_pop': str.strip, 'nk_hostname': str.strip, 'type': str.strip, 'nk_pid': str.strip,'product_name': str.strip, 'nk_serial': str.strip, 'sw_version': str.strip, 'FUNCTION': str.strip,'RACK': int() }, dtype={'RACK': 'Int32', 'SLOT': 'Int32', 'PA': 'Int32'})
pid_df=pd.DataFrame()
pid_eol_df=pd.DataFrame()
eol_dict=pd.DataFrame()
eol_df=pd.DataFrame()
eol_pre_df=pd.DataFrame()



try:
    connection = psycopg2.connect(user="postgres",
                                  password="postgres",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="PUC")
    cursor = connection.cursor()
    # Fetch result
    cursor.execute("SELECT * from dim_product")
    record = cursor.fetchall()
    pid_df=pd.DataFrame(record)
    
    cursor.execute("SELECT * from dim_eol")
    record = cursor.fetchall()
    eol_pre_df=pd.DataFrame(record)
    
    
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        #print("PostgreSQL connection is closed")


pid_list = pid_df[1].values.tolist()




pid_eol_df['Product Id']=pid_list
pid_eol_df['EoL Bolletin']=""
#eol_df['EoL Bolletin']=""
pid_eol_df['Eol URL']=""
#eol_df['EoL Bolletin']=""
pid_eol_df['EoL Announce']=""
#eol_df['EoL Announce']=""
pid_eol_df['EoL End of Sale']=""
#eol_df['EoL End of Sale']=""
pid_eol_df['EoL End SW Maintenance']=""
#pid_df['EoL End of Sec&Vulnerability Fix']=""
pid_eol_df['EoL Last Day of Support']=""
#eol_df['EoL Last Day of Support']=""




# defining the api-endpoint 
API_ENDPOINT = "https://cloudsso.cisco.com/as/token.oauth2"
  
# your API key here
API_CLIENT = "devsecops-eolcenter_prd_gen"
API_KEY = "iZsFSXy3Z8DInyKzhJN1zm25RPB4VuENATaVKtDrEq43GNAUkHYr0IvJdArWEmqf"
  
# data to be sent to api
data = {'grant_type': 'client_credentials',
        'client_id':API_CLIENT,
        'client_secret':API_KEY}
  
# sending post request and saving response as response object
r = requests.post(url = API_ENDPOINT, data = data)
  
# extracting response text 
key_str = r.text
key_str_dict=json.loads(key_str)
access_token=key_str_dict['access_token']
token_type=key_str_dict['token_type']
token_exp=key_str_dict['expires_in']

print(access_token)
# xxx=input("Digite o Token de Acesso")


#HW_pid="ASR1001-X"

API_URL="https://eolcenter-api.cisco.com/api/ext/v1/search/?"

#s = requests.Session()
#s.auth = ('Bearer', access_token)
#s.headers.update({'x-test': 'true'})
#s.parameters = {'criteria': 'Platform','searchType':'Exact','value': HW_pid}
#s.get(API_URL, headers={'x-test2': 'true'}, auth=('Bearer', access_token))

eol_headers = {"Authorization": "Bearer " + access_token}
#HW_pid="ASR1001-X"
#API_URL="https://eolcenter-api.cisco.com/api/ext/v1/search/?"
# parameters to be sent to api
#getEolParameters = {'criteria': 'PID',
#                    'searchType':'Exact',
#                    'value': HW_pid}

# sending post request and saving response as response object
# r = requests.get(url = API_URL, params = getEolParameters, auth=('Bearer', access_token))
#r = requests.get(url = API_URL, params = getEolParameters, headers = eol_headers)
#aux_dict=json.loads(r.text)[0]
#print(aux)
#eol_pb_number=aux_dict['PB_NUMBER']
#eol_pb_url=aux_dict['PB_FINAL_URL']
#eol_announce=aux_dict['EO_EXT_ANNOUNCE_DATE']
#eol_sale=aux_dict['EO_SALES_DATE']
#eol_sw_maint=aux_dict['EO_SW_MAINTENANCE_DATE']
#eol_sec_vul=aux_dict['EO_SECURITY_VUL_SUPPORT_DATE']
#eol_ldos =aux_dict['EO_LAST_SUPPORT_DATE']

#print(eol_sale)

# cd C:\Users\alelima\OneDrive - Cisco\Desktop\pyton tests
# pd.read_excel('exportNetworkInventory-EPNM-20220516.xlsx', index_col=0)  
#inventory_df=pd.read_csv('inventory.csv',sep=';')  
#pid_list=inventory_df['Product Id'].unique()
# pid_list=pid_list.sort()
#pid_list_np=np.array(pid_list)
#unique_pid=pid_list_np.size
# arr0 = [0] * unique_pid
#pid_df=pd.DataFrame()
#pid_df['Product Id']=pid_list
#pid_df['EoL Bolletin']=""
#pid_df['Eol URL']=""
#pid_df['EoL Announce']=""
#pid_df['EoL End of Sale']=""
#pid_df['EoL End SW Maintenance']=""
#pid_df['EoL End of Sec&Vulnerability Fix']=""
#pid_df['EoL Last Day of Support']=""



# pid_list_np=np.append(pid_list_np, arr0,axis=1)
#pid_list_np=np.concatenate([pid_list_np, arr0],axis=1)
#pid_list_np=np.insert(pid_list_np, 0, arr0, axis=1)

# inventory_df['EoL Bolletin']=""
# inventory_df['Eol URL']=""
# inventory_df['EoL Announce']=""
# inventory_df['EoL End of Sale']=""
# inventory_df['EoL End SW Maintenance']=""
# inventory_df['EoL End of Sec&Vulnerability Fix']=""
# inventory_df['EoL Last Day of Support']=""
 
a=0
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

for HW_pid in pid_list:
    HW_pid=HW_pid.upper()
    getEolParameters = {'criteria': 'PID',
                        'searchType':'Exact',
                        'value': HW_pid}

    # sending post request and saving response as response object
    r = requests.get(url = API_URL, params = getEolParameters, headers = eol_headers)
    
    aux_dict=json.loads(r.text)
    if aux_dict != []:
        aux_dict=aux_dict[0]
        
        
        #eol_dict['Product Id']=HW_pid
        
        pid_eol_df.loc[a]['EoL Bolletin']=aux_dict['PB_NUMBER']
        #eol_df.loc[a]['EoL Bolletin']=aux_dict['PB_NUMBER']
        #eol_dict['EoL Bolletin']=aux_dict['PB_NUMBER']
        
        
       #eol_df.loc[a]['Eol URL']=aux_dict['PB_FINAL_URL']
        pid_eol_df.loc[a]['Eol URL']=aux_dict['PB_FINAL_URL']
        #eol_dict['Eol URL']=aux_dict['PB_FINAL_URL']
        
        #ol_df.loc[a]['EoL Announce']=aux_dict['EO_EXT_ANNOUNCE_DATE'][0:10]
        pid_eol_df.loc[a]['EoL Announce']=aux_dict['EO_EXT_ANNOUNCE_DATE'][0:10]
        #eol_dict['EoL Announce']=aux_dict['EO_EXT_ANNOUNCE_DATE'][0:10]
        
        #eol_df.loc[a]['EoL End of Sale']=aux_dict['EO_SALES_DATE'][0:10]
        #eol_dict['EoL End of Sale']=aux_dict['EO_SALES_DATE'][0:10]
        pid_eol_df.loc[a]['EoL End of Sale']=aux_dict['EO_SALES_DATE'][0:10]
        
        if aux_dict['EO_SW_MAINTENANCE_DATE'] != None:
            pid_eol_df.loc[a]['EoL End SW Maintenance']=aux_dict['EO_SW_MAINTENANCE_DATE'][0:10]
        else:
            #pid_eol_df.loc[a]['EoL End SW Maintenance']= pid_eol_df.loc[a]['EoL End of Sale']
            pid_eol_df.loc[a]['EoL End SW Maintenance']="2020-01-01"
        
        
        ##pid_df.loc[a]['EoL End of Sec&Vulnerability Fix']=aux_dict['EO_SECURITY_VUL_SUPPORT_DATE']
        #eol_dict['EoL End SW Maintenance']=aux_dict['EO_SW_MAINTENANCE_DATE'][0:10]
        
        #eol_df.loc[a]['EoL Last Day of Support']=aux_dict['EO_LAST_SUPPORT_DATE'][0:10]
        #eol_dict['EoL Last Day of Support']=aux_dict['EO_LAST_SUPPORT_DATE'][0:10]
        pid_eol_df.loc[a]['EoL Last Day of Support']=aux_dict['EO_LAST_SUPPORT_DATE'][0:10]
        #print(eol_dict)
        #pd.concat([eol_df,eol_dict],ignore_index=True)
    else:
        pid_eol_df.loc[a]['EoL Bolletin']=""
        
        pid_eol_df.loc[a]['Eol URL']=""
        
        #eol_df.loc[a]['EoL Announce']=aux_dict['EO_EXT_ANNOUNCE_DATE'][0:10]
        pid_eol_df.loc[a]['EoL Announce']=""
        
        #eol_df.loc[a]['EoL End of Sale']=aux_dict['EO_SALES_DATE'][0:10]
        pid_eol_df.loc[a]['EoL End of Sale']=""
        
        pid_eol_df.loc[a]['EoL End SW Maintenance']=""
        
        
        #eol_df.loc[a]['EoL Last Day of Support']=aux_dict['EO_LAST_SUPPORT_DATE'][0:10]
        pid_eol_df.loc[a]['EoL Last Day of Support']=""
    a=a+1
    

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

# pid_df.sort_index(axis = 1)

#eol_df=pid_df.sort_values(by=['Product Id'])
# os.chdir('C:\Users\alelima\OneDrive - Cisco\PUC\Classroom\2023-1-PROJ\CSV input\PUC modified')

#pid_eol_df.to_csv('eol_pid-DatabaseTemp.csv',sep=';', index=False)
pid_eol_df.to_csv(r'C:\Users\alelima\OneDrive - Cisco\PUC\Classroom\2023-1-PROJ\CSV input\PUC modified\eol_pid-DatabaseTemp.csv',sep=';', index=False)

#pid_eol_df[['EoL Bolletin','Eol URL']]

tempdf=pid_eol_df[['EoL Bolletin','Eol URL', 'EoL Announce','EoL End of Sale','EoL End SW Maintenance','EoL Last Day of Support']]
tempdf=tempdf.drop_duplicates()

eol_df['EoL Bolletin']=tempdf['EoL Bolletin']
eol_df['Eol URL']=tempdf['Eol URL']
eol_df['EoL Announce']=tempdf['EoL Announce']
eol_df['EoL End of Sale']=tempdf['EoL End of Sale']
eol_df['EoL End SW Maintenance']=tempdf['EoL End SW Maintenance']
eol_df['EoL Last Day of Support']=tempdf['EoL Last Day of Support']
eol_df.reset_index(inplace=True, drop=True)
eol_insert= eol_df.values.tolist()

#len(eol_df.loc[1]['EoL Bolletin'])



current_time = now.strftime("%Y-%m-%d")


try:
    connection = psycopg2.connect(user="postgres",
                                  password="postgres",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="PUC")
    cursor = connection.cursor()
    connection.autocommit = True



    # UPDATE DIMENSION EOL (dim_eol)
    for pb_index, eol_pb in enumerate(eol_df['EoL Bolletin']):
        cursor.execute(f"SELECT * from dim_eol WHERE nk_bulletin  = '{eol_pb}'");
        pb_record=cursor.fetchall()
        #print("loop", bb)
        if eol_pb=='':
            #print("nullpb", bb)
            continue
        elif len(pb_record)>0:
            #print("existingpb", bb)
            if pb_record[0][2]!=eol_df.loc[pb_index]['Eol URL']:
                cursor.execute(f"UPDATE dim_eol set eol_url = '{eol_df.loc[pb_index]['Eol URL']}' WHERE nk_bulletin  = '{eol_pb}'");
            if str(pb_record[0][3])!=eol_df.loc[pb_index]['EoL Announce']:
                cursor.execute(f"UPDATE dim_eol set eol_announce = '{eol_df.loc[pb_index]['EoL Announce']}' WHERE nk_bulletin  = '{eol_pb}'");
            if str(pb_record[0][4])!=eol_df.loc[pb_index]['EoL End of Sale']:
                cursor.execute(f"UPDATE dim_eol set eol_sale = '{eol_df.loc[pb_index]['EoL End of Sale']}' WHERE nk_bulletin  = '{eol_pb}'");
            if str(pb_record[0][5])!=eol_df.loc[pb_index]['EoL End SW Maintenance']:
                cursor.execute(f"UPDATE dim_eol set eoswm = '{eol_df.loc[pb_index]['EoL End SW Maintenance']}' WHERE nk_bulletin  = '{eol_pb}'");
            if str(pb_record[0][6])!=eol_df.loc[pb_index]['EoL Last Day of Support']:
                cursor.execute(f"UPDATE dim_eol set ldos = '{eol_df.loc[pb_index]['EoL Last Day of Support']}' WHERE nk_bulletin  = '{eol_pb}'");
        else:
            # insert new field into table
            #print("newpb", bb)
            #insertquery = """INSERT INTO dim_eol (nk_bulletin, eol_url, eol_announce, eol_sale, eoswm, ldos) VALUES ('EOL12504', 'https://www.cisco.com/c/en/us/products/collateral/routers/carrier-routing-system/eos-eol-notice-c51-741155.html', '30/07/2018', '30/07/2019', '01/01/2000','31/07/2024')"""
            cursor.execute(f"INSERT INTO dim_eol (nk_bulletin, eol_url, eol_announce, eol_sale, eoswm, ldos) VALUES  ('{eol_pb}','{eol_df.loc[pb_index]['Eol URL']}','{eol_df.loc[pb_index]['EoL Announce']}','{eol_df.loc[pb_index]['EoL End of Sale']}','{eol_df.loc[pb_index]['EoL End SW Maintenance']}','{eol_df.loc[pb_index]['EoL Last Day of Support']}')");
        #bb = bb + 1

except (Exception, psycopg2.Error) as error:
   print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


print('Execution Completed')

