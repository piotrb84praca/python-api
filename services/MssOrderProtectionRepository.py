import pandas as pd
import pyreadstat
import sys
import os 

data_sas = os.path.join('/home/app_digit/JZRR/',  'decyzje.sas7bdat')

class MssOrderProtectionRepository:

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
          
            result = data[(data['CRMFIX']==params['telka'])]
            result = result[['CRMFIX','sales_channel_1','salesman_code_1','sales_channel_2','salesman_code_2','miesiac_skompletowania']]
            
            # Creating the desired array of dictionaries
            result_array = [
                {col: i for col in result.columns}
                for i in range(len(result))
            ]            
  
            data=[]
            for index, item in enumerate(result_array):                   
               
                data_list = {
                    'tel': result.iloc[index,0],
                    'sales channel 1': result.iloc[index,1],
                    'salesmancode 1': result.iloc[index,2],
                    'sales channel 2': result.iloc[index,3],
                    'salesman code 2': result.iloc[index,4],
                    'miesiac skompletowania': result.iloc[index,5]
                }
                #print(data_list)
                data.append(data_list)  

            return ({"data": data})
        except Exception as e:
            print(f"Error loading SAS file: {e}")
            return None