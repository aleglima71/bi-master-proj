# -*- coding: utf-8 -*-
"""
Created on Sun May  7 14:26:14 2023

@author: alelima
"""

import pandas as pd
import psycopg2
from datetime import datetime



inventory_df=pd.read_csv(r'C:\Users\alelima\OneDrive - Cisco\PUC\Classroom\2023-1-PROJ\CSV input\PUC modified\DatabaseTemp.csv',sep=';', converters={'nk_pop': str.strip, 'nk_hostname': str.strip, 'type': str.strip, 'nk_pid': str.strip,'product_name': str.strip, 'nk_serial': str.strip, 'sw_version': str.strip, 'FUNCTION': str.strip,'RACK': int() }, dtype={'RACK': 'Int32', 'SLOT': 'Int32', 'PA': 'Int32'})
pid_eol_df=pd.read_csv(r'C:\Users\alelima\OneDrive - Cisco\PUC\Classroom\2023-1-PROJ\CSV input\PUC modified\eol_pid-DatabaseTemp.csv',sep=';', converters={'Product Id': str.strip, 'EoL Bolletin': str.strip, 'Eol URL': str.strip})

now = datetime.now()
current_time = now.strftime("%Y-%m-%d")


try:
    connection = psycopg2.connect(user="postgres",
                                  password="postgres",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="PUC")
    cursor = connection.cursor()
    connection.autocommit = True
    
    cursor.execute(f"SELECT sk_time from dim_time WHERE nk_date  = '{current_time}'");
    record=cursor.fetchall()
    if len(record)>0:
        sk_time=record[0][0]
    else:
        year= int(now.strftime("%Y"))
        month= int(now.strftime("%m"))
        day= int(now.strftime("%d"))
        cursor.execute(f"INSERT INTO dim_time (nk_date, year, month, day) VALUES  ('{current_time}','{year}','{month}','{day}')");
        cursor.execute(f"SELECT sk_time from dim_time WHERE nk_date  = '{current_time}'");
        record=cursor.fetchall()
        sk_time=record[0][0]
    cursor.execute(f"SELECT MAX(eolpid_version) from ft_eolpid");
    record=cursor.fetchall()
    if record[0][0] != None:
        eolpid_version=int(record[0][0])+1
    else:
        eolpid_version=1
    # UPDATE FACT EOL (ft_eolpid)
    for pid_index, HW_pid in enumerate(pid_eol_df['Product Id']):
        cursor.execute(f"SELECT sk_product from dim_product WHERE nk_pid  = '{HW_pid}'");
        record=cursor.fetchall()
        sk_product=record[0][0]
            
        if pid_eol_df.loc[pid_index]['EoL Bolletin'] != '' :
            cursor.execute(f"SELECT sk_eol from dim_eol WHERE nk_bulletin  = '{pid_eol_df.loc[pid_index]['EoL Bolletin']}'");
            record=cursor.fetchall()
            sk_eol=record[0][0]
            cursor.execute(f"SELECT sk_eolpid from ft_eolpid WHERE sk_product = {sk_product} AND sk_eol = {sk_eol}");
            record=cursor.fetchall()
            if len(record)==0:
                # INSERT DATA INTO FT_EOLPID
                cursor.execute(f"INSERT INTO ft_eolpid (sk_eol, sk_product, sk_time,eolpid_version) VALUES  ({sk_eol},{sk_product},{sk_time},{eolpid_version})");
                continue
            
        continue
        
    cursor.execute(f"SELECT MAX(node_version) from ft_node");
    record=cursor.fetchall()
    if record[0][0] != None:
        node_version=int(record[0][0])+1
    else:
        node_version=1
    cursor.execute(f"SELECT MAX(inventory_version) from ft_inventory");
    record=cursor.fetchall()
    if record[0][0] != None:
        inventory_version=int(record[0][0])+1
    else:
        inventory_version=1

    for row in range(len(inventory_df)):
        nk_pop=inventory_df.loc[row]['nk_pop']
        nk_hostname=inventory_df.loc[row]['nk_hostname']
        nk_pid=inventory_df.loc[row]['nk_pid']
        nk_serial=inventory_df.loc[row]['nk_serial']
        rack_number=inventory_df.loc[row]['RACK']
        slot_number=inventory_df.loc[row]['SLOT']
        subslot_number=inventory_df.loc[row]['PA']
        
        
        cursor.execute(f"SELECT sk_hardware from dim_hardware WHERE nk_serial  = '{inventory_df.loc[row]['nk_serial']}'");
        record=cursor.fetchall()
        sk_hardware=record[0][0]
        
        cursor.execute(f"SELECT sk_product from dim_product WHERE nk_pid  = '{inventory_df.loc[row]['nk_pid']}'");
        record=cursor.fetchall()
        sk_product=record[0][0]
        
       
        cursor.execute(f"SELECT sk_device from dim_device WHERE nk_hostname  = '{inventory_df.loc[row]['nk_hostname']}'");
        # atencao se SW version mudou na tabela, ajustar
        record=cursor.fetchall()
        sk_device=record[0][0]
        
        # INCLUIR SLOT NA CONSULTA
        cursor.execute(f"SELECT sk_inventory from ft_inventory WHERE sk_product = {sk_product} AND sk_hardware = {sk_hardware}");
        record=cursor.fetchall()
        if len(record)==0:
            # INSERT DATA INTO FT_INVENTORY
            #print(sk_device)
            #cursor.execute(f"INSERT INTO ft_inventory (sk_product, sk_hardware, sk_time, inventory_version) VALUES  ({sk_product},{sk_hardware},{sk_time}, {inventory_version})");
            cursor.execute(f"INSERT INTO ft_inventory (sk_product, sk_hardware, sk_device, sk_time, inventory_version) VALUES  ({sk_product},{sk_hardware},{sk_device},{sk_time}, {inventory_version})");
    
 
        
        if inventory_df.loc[row]['type']=="CHASSIS":
            cursor.execute(f"SELECT sk_pop from dim_pop WHERE nk_pop  = '{inventory_df.loc[row]['nk_pop']}'");
            record=cursor.fetchall()
            sk_pop=record[0][0]
            
            
            cursor.execute(f"SELECT sk_node from ft_node WHERE sk_pop = {sk_pop} AND sk_device = {sk_device}");
            record=cursor.fetchall()
            if len(record)==0:
                # INSERT DATA INTO FT_NODE
                #print(sk_device)
                #cursor.execute(f"INSERT INTO ft_node (sk_pop, sk_device, sk_hardware, sk_time, node_version) VALUES  ({sk_pop},{sk_device},{sk_hardware},{sk_time}, {node_version})");
                cursor.execute(f"INSERT INTO ft_node (sk_pop, sk_device, sk_time, node_version) VALUES  ({sk_pop},{sk_device},{sk_time}, {node_version})");
        
except (Exception, psycopg2.Error) as error:
   print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


print('Execution Completed')

