import config.config  as config
from datetime import datetime
import os

class FileUpdatesRepository:
    def validate(self, params):
         return True, None
        
    def get_data(self, params):
        # List to store file update times
            data = []
            directory = '/home/app_digit/JZRR/'
            # Iterate through the files in the directory
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                if os.path.isfile(file_path):  # Check if it's a file
                    # Get the last modified time
                    update_time = os.path.getmtime(file_path)
                    # Convert to a readable format
                    readable_time = datetime.fromtimestamp(update_time).strftime('%Y-%m-%d %H:%M:%S')
                    # Append to the data array
                    data.append({'filename': filename, 'last_updated': readable_time})
                   
                    
            return ({"data": data})