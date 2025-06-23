import pandas as pd
import pyreadstat
import sys
import os 

data_sas = os.path.join('/home/app_digit/JZRR/',  'crm_mobile.sas7bdat')

class LmeMobMsisdnRepository:

    def validate(self, params):
            required_params = ['msisdn']  
            
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
          
            #concMsisdn = '214081693|214081693|214077864|500177828|502700194|503031807|508356300|516059411|516059412|516151559'       
            params = {'msisdn': params['msisdn'].rstrip('|')}
            msisdn_list = params['msisdn'].split('|')
            result = data[data['KON_MSISDN'].isin(msisdn_list)][['KLI_NIP','KLI_REGON','KON_MSISDN','data_aktywacji','dlugosc_lojalki','kod_dealera']]  

            # Creating the desired array of dictionaries
            result_array = [
                {col: i for col in result.columns}
                for i in range(len(result))
            ]            
  
            data=[]
            for index, item in enumerate(result_array):                   
               
                data_list = {
                    'KLI_NIP': result.iloc[index,0],
                    'KLI_REGON': result.iloc[index,1],
                    'KON_MSISDN': result.iloc[index,2],
                    'data_aktywacji': result.iloc[index,3].strftime('%Y-%m-%d'),
                    'dlugosc_lojalki': result.iloc[index,4],
                    'kod_dealera': result.iloc[index,5]
                }
                #print(data_list)
                data.append(data_list)  
            print(data)
            return ({"data": data})
        except Exception as e:
            print(f"Error loading SAS file: {e}")
            return None