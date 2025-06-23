import pandas as pd
import pyreadstat
import sys
import os 

data_sas = os.path.join('/home/app_digit/JZRR/',  'wl_sokx.sas7bdat')

class WlSOKxRepository:

    def validate(self, params):
            required_params = ['ord_number']  
            
            for param in required_params:
                if param not in params:
                    return False, f"Missing required parameter: {param}"        
    
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
                
            result=data[(data['NR_ZAMOWIENIA']==params['ord_number']) ]             
            
            # Creating the desired array of dictionaries
            result_array = [
                {col: i for col in result.columns}
                for i in range(len(result))
            ]            
  
            data=[]
            for index, item in enumerate(result_array):                  

                data_dict = {}  
                for index2, columnName in enumerate(result_array[0]):         
                        data_dict[columnName] = result.iloc[index, index2]                 

                data.append(data_dict)  
                
            return ({"data": data})
        except Exception as e:
            print(f"Error loading SAS file: {e}")
            return None