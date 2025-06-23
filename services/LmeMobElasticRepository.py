import pandas as pd
import pyreadstat
import sys
import os 
import re
#https://klik-b2b-api.corpnet.pl:12130/service/LmeMobElastic?number=51319/1
data_sas = os.path.join('/home/app_digit/JZRR/',  'raport_elastyczna.sas7bdat')

class LmeMobElasticRepository:

    def validate(self, params):
            required_params = ['ident_number']  
            
            for param in required_params:
                if param not in params:
                    return False, f"Missing required parameter: {param}"        
                    
                ident_number = params['ident_number']                
                if not re.match(r'^\d{5}/\d$', ident_number):
                    return False, "Invalid ident_number format. It should be in the format xxxxx/y, where xxxxx is a 5-digit number and y is a single digit."
                    
            return True, None
        
    def get_data(self, params):
        try:          
            global data_sas              
        
            is_valid, error_message = self.validate(params)
            if not is_valid:
                return {'error': error_message}           
                
            data, meta = pyreadstat.read_sas7bdat(data_sas)
            #print(data)
            #print(meta)
            if data is None:
                return None  
            
            result=data[(data['NumerOferty']==params['ident_number']) ]             
            #print(data)
            # Creating the desired array of dictionaries

            '''result_array = [
                {col: i for col in result.columns}
                for i in range(len(result))
            ]            
  
            data=[]
            for index, item in enumerate(result_array):                  

                data_dict = {}  
                for index2, columnName in enumerate(result_array[0]):         
                        data_dict[columnName] = result.iloc[index, index2]                 

                data.append(data_dict)  
                
            return ({"data": data})'''
            
            result_array = [
                {col: i for col in result.columns}
                for i in range(len(result))
            ]            
  
            data=[]
            for index, item in enumerate(result_array):                   
               
                data_list = {
                    'Data wygenerowania': result.iloc[index,0].strftime('%Y-%m-%d %H:%M:%S'),
                    'Numer Oferty': result.iloc[index,1],
                    'Zobowiazanie Miesięczne': result.iloc[index,2],
                    'Plan Firma Abonament': result.iloc[index,3],
                    'Marża': result.iloc[index,4],
                    'Prowizja': result.iloc[index,5]
                }
                #print(data_list)
                data.append(data_list)  

            return ({"data": data})
        except Exception as e:
            print(f"Error loading SAS file: {e}")
            return None