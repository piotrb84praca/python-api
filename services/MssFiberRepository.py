import pandas as pd
import pyreadstat
import sys
import os 
import re
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
#https://klik-b2b-api.corpnet.pl:12130/service/MssFiber?telka=TEL000142746547

data_sas = os.path.join('/home/app_digit/JZRR/',  'zainstalowane_produkty.sas7bdat')
data_sas_facorder =  os.path.join('/home/app_digit/JZRR/',  'fac_order_customer.sas7bdat') 
data_sas_fiber =  os.path.join('/home/app_digit/JZRR/',  'fiber_swietlik.sas7bdat') 

class MssFiberRepository:

    def validate(self, params):
            required_params = ['telka']
            
            for param in required_params:
                if param not in params:
                    return False, f"Missing required parameter: {param}"        
                 
                if not params['telka'].startswith("TEL"):
                    return False, f"Parameter '{param}' must start with 'TEL'."
                
            return True, None
        
            
    def get_data(self, params):
        try:          
            global data_sas          
            global data_sas_facorder              
            
            is_valid, error_message = self.validate(params)
            if not is_valid:
                return {'error': error_message}           
                
            data, meta = pyreadstat.read_sas7bdat(data_sas)
            data_facorder, meta_facorder = pyreadstat.read_sas7bdat(data_sas_facorder)
            data_fiber, meta_fiber = pyreadstat.read_sas7bdat(data_sas_fiber)
            
            if data is None:
                return None  
                
            if data_facorder is None:
                return None 
                
            if data_fiber is None:
                return None    
                
            facorder_f=data_facorder[(data_facorder['EXTERNAL_ID']==params['telka']) ]
            distinct_cust_bo_id_txt = facorder_f['cust_bo_id_txt'].drop_duplicates()
            #print(distinct_cust_bo_id_txt)
            result=data[(data['cust_bo_id_txt'].isin(distinct_cust_bo_id_txt) ) ] 
            
            ##prod data
            unique_addresses=result[['installation_town', 'installation_street', 'installation_number', 'installation_zipcode']].drop_duplicates()
            unique_addresses['installation_zipcode']=unique_addresses['installation_zipcode'].str.replace('-', '', regex=False)
            
            #print(unique_addresses[['installation_town', 'installation_street', 'installation_number', 'installation_zipcode']])
            unique_addresses = unique_addresses.rename(
                columns={
                    'installation_town': 'miasto',
                    'installation_street': 'ulica',
                    'installation_number': 'nr_domu',
                    'installation_zipcode': 'KODPOCZT'
                }
            )
            
            result = pd.merge(data_fiber, unique_addresses, on=['miasto', 'ulica', 'nr_domu', 'KODPOCZT'], how='inner')           
            #print(result[['miasto', 'ulica', 'nr_domu', 'KODPOCZT']])

            # Creating the desired array of dictionaries
            result_array = [
                {col: i for col in result.columns}
                for i in range(len(result))
            ]            
            result = result.fillna(pd.Timestamp('2000-01-01'))
            data=[]
            for index, item in enumerate(result_array):                   
                
                data_list = {
                    'Wojwewodztwo': result.iloc[index,0],
                    'Powiat': result.iloc[index,1],
                    'Miasto': result.iloc[index,2],
                    'Kod pocztowy': result.iloc[index,3],
                    'Ulica': result.iloc[index,4],
                    'Nr domu': result.iloc[index,5],
                    'Data testlab ': result.iloc[index,6],
                    'Id posesja OPL': result.iloc[index,7],
                }
                #print(data_list)
                data.append(data_list)  

            return ({"data": data})
        except Exception as e:
            print(f"Error loading SAS file: {e}")
            return None