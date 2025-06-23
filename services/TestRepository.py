import config.config  as config
from datetime import datetime
import pymssql

class TestRepository:
     def __init__(self):
           pass
     def validate(self, params):
        required_params = []  
        
        for param in required_params:
            if param not in params:
                return False, f"Missing required parameter: {param}"
        
        try:
            date_from = datetime.strptime(params.get('date_from'), '%Y-%m-%d')
            date_to = datetime.strptime(params.get('date_to'), '%Y-%m-%d')
        except ValueError:
            return False, "Invalid date format. Expected YYYY-MM-DD for 'date_from' and 'date_to'."

        return True, None
         
     def get_data(self, params):

        is_valid, error_message = self.validate(params)
        if not is_valid:
            return {'error': error_message}
        try:                         
                conn = pymssql.connect(host=config.MSSQLHOST,
                                       user=config.MSSQLLOGIN,
                                       password=config.MSSQLPASSWORD,
                                       charset='UTF-8', database=config.MSSQLDB, tds_version=r'7.0')  

                query = "SELECT @@VERSION"  
         
                cursor = conn.cursor()  
                cursor.execute(query)    
                results = cursor.fetchall()

                data = []
                # Define keys for the columns
                keys = [column[0] for column in cursor.description]
                
                # Iterate through the result and map values to keys
                for i, row in enumerate(results):
                    # Create a dictionary for each row
                    row_dict = {keys[j]: row[j] for j in range(len(row))}
                    
                    # Append the dictionary to the data list
                    data.append(row_dict)
                
                # Print the formatted data
                print(data)    
                return ({"data": data})
                return ("Test ok")
        except Exception as e:
                return {'error': str(e)}
        finally:
                conn.close()  
        