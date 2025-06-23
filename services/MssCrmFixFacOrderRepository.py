import pandas as pd
import pyreadstat
import sys
import os 
import re
#https://klik-b2b-api.corpnet.pl:12130/service/MssCrmFixFacOrder?telka=TEL000142746547

data_sas =  os.path.join('/home/app_digit/JZRR/',  'fac_order_customer.sas7bdat') 

class MssCrmFixFacOrderRepository:

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
            
            is_valid, error_message = self.validate(params)
            if not is_valid:
                return {'error': error_message}           
                
            data, meta = pyreadstat.read_sas7bdat(data_sas)

            if data is None:
                return None   
                
            result=data[(data['EXTERNAL_ID']==params['telka']) ]     
         
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
                    'External Id': result.iloc[index,0],
                    'Capture Id': result.iloc[index,1],
                    'Capture Dt': result.iloc[index,2].strftime('%Y-%m-%d'),
                    'Capture Status': result.iloc[index,3],
                    'Capture Status Dsc': result.iloc[index,4],
                    'Installation Town': result.iloc[index,5],
                    'Installation Street': result.iloc[index,6],
                    'Installation Number': result.iloc[index,7],
                    'Installation Flat': result.iloc[index,8],
                    'Installation Code': result.iloc[index,9],
                    'Design num Code': result.iloc[index,10],    
                    'Service Req Dt': result.iloc[index,11].strftime('%Y-%m-%d'),                    
                    'Contract Dt': result.iloc[index,12].strftime('%Y-%m-%d'),
                    'Service Inst Dt': result.iloc[index,13].strftime('%Y-%m-%d'),
                    'Billing Dt': result.iloc[index,14].strftime('%Y-%m-%d'),
                    'Cust Id': result.iloc[index,15],
                    'Nip': result.iloc[index,16],
                    'Cop Tp Regon': result.iloc[index,17],
                    #'Business Prj Dt': result.iloc[index,18],
                    'Partner': result.iloc[index,19],
                    'Line Action end': result.iloc[index,20],
                    'Promo Nbr': result.iloc[index,21],
                    'Loyalty start': result.iloc[index,22].strftime('%Y-%m-%d'),
                    'Loyalty end': result.iloc[index,23].strftime('%Y-%m-%d'),
                    'Promotion name': result.iloc[index,24]                                
                }
                #print(data_list)
                data.append(data_list)  

            return ({"data": data})
        except Exception as e:
            print(f"Error loading SAS file: {e}")
            return None