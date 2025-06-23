import pandas as pd
import pyreadstat
import sys
import os 

#https://klik-b2b-api.corpnet.pl:12130/service/LmeMobKws?date_from=2025-01-01&type=akc&kws_number=ALC_1B_2022_DS
KWSY_sas = os.path.join('/home/app_digit/JZRR/',  'kwsy_historia_total.sas7bdat')
KWSY_akc_sas = os.path.join('/home/app_digit/JZRR/',  'kwsy_akc_total.sas7bdat')

class LmeMobKwsRepository:

    def validate(self, params):
        required_params = ['kws_number','date_from','type']  
        
        for param in required_params:
            if param not in params:
                return False, f"Missing required parameter: {param}"        

        return True, None
        
    ######################################## KWS ########################################   
    def get_data(self, params):
        try:

            is_valid, error_message = self.validate(params)
            if not is_valid:
                return {'error': error_message}           
                
            if params['type']=='akc':
                global KWSY_akc_sas    
            else:
                global KWSY_sas    
        
            data, meta = pyreadstat.read_sas7bdat(KWSY_sas)
            print(data)
            print(meta)
            if data is None:
                return None  
                
            data['kws_date_od'] = pd.to_datetime(data['kws_date_od'], errors='coerce')
            data['kws_date_do'] = pd.to_datetime(data['kws_date_do'], errors='coerce')
            data['kws_date_do'].fillna(pd.Timestamp('2100-01-01'), inplace=True)    
            data = data.drop(columns=['data_od', 'Aktualne_Portfolio'], errors='ignore')
            #doing filtering
            result = data[(data['Kod_BSCS']==params['kws_number'])]
            result = result[(result['kws_date_od']>=pd.to_datetime(params['date_from']))]
         
            
            # Creating the desired array of dictionaries
            result_array = [
                {col: i for col in result.columns}
                for i in range(len(result))
            ]            
  
            data=[]
            for index, item in enumerate(result_array): 
                data_dict = {}  
                for index2, columnName in enumerate(result_array[0]):  
                        if isinstance(result.iloc[index, index2], pd.Timestamp):
                            data_dict[columnName] = result.iloc[index, index2].strftime('%Y-%m-%d')                 
                        else:    
                            data_dict[columnName] = result.iloc[index, index2]   

                data.append(data_dict)  
                
            return ({"data": data})
            
        except Exception as e:
            print(f"Error loading SAS file: {e}")
            return None