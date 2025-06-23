import pandas as pd
import pyreadstat
import sys
import os 
import re
#https://klik-b2b-api.corpnet.pl:12130/service/MssCrmFixProd?telka=TEL000142746547

data_sas = os.path.join('/home/app_digit/JZRR/',  'zainstalowane_produkty.sas7bdat')
data_sas_facorder =  os.path.join('/home/app_digit/JZRR/',  'fac_order_customer.sas7bdat') 

class MssCrmFixProdRepository:

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
            
            if data is None:
                return None  
                
            if data_facorder is None:
                return None     
                
            facorder_f=data_facorder[(data_facorder['EXTERNAL_ID']==params['telka']) ]
            result=data[(data['cust_bo_id_txt'].isin(facorder_f['cust_bo_id_txt']) ) ] 
            #print(facorder_f)
            #print(result)
            # Creating the desired array of dictionaries
            result_array = [
                {col: i for col in result.columns}
                for i in range(len(result))
            ]            
            result = result.fillna(pd.Timestamp('2000-01-01'))
            data=[]
            for index, item in enumerate(result_array):                   
                
                data_list = {
                    'Id': result.iloc[index,0],
                    'Installation Id': result.iloc[index,1],
                    'Installation Status': result.iloc[index,2],
                    'Product Id': result.iloc[index,3],
                    'Number': result.iloc[index,4],
                    'Description': result.iloc[index,5],
                    #'TP_TOPLVL_INST_PRD': result.iloc[index,6],
                    'Loyalty start': result.iloc[index,7].strftime('%Y-%m-%d'),
                    'Loyalty end': result.iloc[index,8].strftime('%Y-%m-%d'),
                    'Promotion name': result.iloc[index,9],
                    'Capture Id': result.iloc[index,10],
                    'Installation town': result.iloc[index,11],
                    'Installation street': result.iloc[index,12],
                    'Installation number': result.iloc[index,13],
                    'Installation flat': result.iloc[index,14],
                    'Installation code': result.iloc[index,15],
                    'Tariff': result.iloc[index,16],
                    'Opcja': result.iloc[index,17],
                    'Terminated date':  result.iloc[index,18].strftime('%Y-%m-%d')                
                }
                #print(data_list)
                data.append(data_list)  

            return ({"data": data})
        except Exception as e:
            print(f"Error loading SAS file: {e}")
            return None