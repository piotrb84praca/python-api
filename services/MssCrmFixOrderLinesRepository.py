import pandas as pd
import pyreadstat
import sys
import os 
import re
#https://klik-b2b-api.corpnet.pl:12130/service/MssCrmFixOrderLines?telka=TEL000142746547

data_sas =  os.path.join('/home/app_digit/JZRR/',  'order_lines.sas7bdat') 

class MssCrmFixOrderLinesRepository:

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
                    'Line Action Dt': result.iloc[index,2],
                    'Loyalty start': result.iloc[index,3].strftime('%Y-%m-%d'),
                    'Loyalty end': result.iloc[index,4].strftime('%Y-%m-%d'),                   
                    'Promotion Name': result.iloc[index,5],                   
                    'Option': result.iloc[index,6]                                
                }
                #print(data_list)
                data.append(data_list)  

            return ({"data": data})
        except Exception as e:
            print(f"Error loading SAS file: {e}")
            return None