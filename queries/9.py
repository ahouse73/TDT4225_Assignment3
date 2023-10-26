from pprint import pprint 
from DbConnector import DbConnector
from decouple import config
from datetime import datetime

# Find all users who have invalid activities, and the number of invalid activities per user
# https://stackoverflow.com/questions/32264225/how-to-get-multiple-document-using-array-of-mongodb-id
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        
        # 1. Per activity, get all the trackpoints
        # 2. Check if they qualify to be invalid
        # 3. If invalid, add to dict
        
        activities = self.db['Activity'].find()
        
        invalid_users_dict = {} # Bookkeeping
        for act in activities:
            user_id = act['user_id']
            trackpoints = list(self.db['TrackPoint'].find({"_id":{"$in":act['trackpoints']}}, {'Date':1, '_id':1})) # This returns all sequential trackpoints
            
            
            date_format = '%Y-%m-%dT%H:%M:%S.%f'
            for index, tp in enumerate(trackpoints):
                
                if index == 0: # First trackpoint does not have anything to compare to
                    prev_timestamp = datetime.strptime(tp['Date'], date_format)
                    continue
                
                current_timestamp = datetime.strptime(tp['Date'], date_format)
                
                if (current_timestamp - prev_timestamp).total_seconds() > 300:
                    if user_id in invalid_users_dict:
                        invalid_users_dict[user_id] += 1
                    else:
                        invalid_users_dict[user_id] = 1
                        #pprint(user_id)
                
                prev_timestamp = current_timestamp
  
        pprint(invalid_users_dict)
        self.close()    
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

