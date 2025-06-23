import pandas as pd
import pyreadstat
import sys
import os 
import glob

#"https://klik-b2b-api.corpnet.pl:12130/service/MssPotential?msisdn=453683122|453683123|453683124|453683125&date_from=2025-04-01" 

class MssPotentialRepository:

    def validate(self, params):
            required_params = ['msisdn','date_to'] 
            
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
         
            year,month,day = params['date_to'].split('-')
            
            data_sas = os.path.join('/home/app_digit/JZRR/', f'potencjal_retencja_am_{year}{month}.sas7bdat') 
            file_date = year+' '+month
            
            # Check if the file exists
            if os.path.exists(data_sas):
                data, meta = pyreadstat.read_sas7bdat(data_sas)
            else:
                # Get the last file from the folder that matches the pattern
                files = glob.glob('/home/app_digit/JZRR/potencjal_retencja_am_*.sas7bdat')
                if files:
                    last_file = max(files, key=os.path.getctime)  # Get the most recent file
                    path, file = last_file.split('20')
                    date, ext  = file.split('.')
                    file_date  = '20'+date
                    data, meta = pyreadstat.read_sas7bdat(last_file)
                else:
                    print("No files found.")
              
            if data is None:
                return None
               
            params = {'msisdn': params['msisdn'].rstrip('|')}
            msisdn_list = params['msisdn'].split('|')
            result = data[data['MSISDN'].isin(msisdn_list)]
            
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
                            data_dict[columnName.replace("_", " ").title()] = result.iloc[index, index2].strftime('%Y-%m-%d')                
                        elif columnName =='kon_promotion_end':
                            data_dict[columnName.replace("_", " ").title()] = result.iloc[index, index2].strftime('%Y-%m-%d')                 
                        else: 
                            data_dict['Rok miesiąc danych'] =  file_date
                            data_dict[columnName.replace("_", " ").title()] = result.iloc[index, index2]   

                data.append(data_dict)  
                
            return ({"data": data})
        except Exception as e:
            print(f"Error loading SAS file: {e}")
            return None