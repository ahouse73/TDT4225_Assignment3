from pprint import pprint 
from DbConnector import DbConnector
from decouple import config
from datetime import datetime

# Find all users who have invalid activities, and the number of invalid activities per user
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        
        # 1. Per Activity, get all trackpoints
        activities = self.db['Activity'].find()
        invalid_users_dict = {}
        for act in activities:
            user_id = act['user_id']
            trackpoints = act['trackpoints']
            amount_of_trackpoints = len(trackpoints)
        
            date_format = '%Y-%m-%dT%H:%M:%S.%f'
            for index, trackpoint_id in enumerate(trackpoints):
                # To prevent index out of bounds error
                if index == 0:
                    current_trackpoint = self.db['TrackPoint'].find_one({"_id":trackpoint_id})
                    current_trackpoint_timestamp = datetime.strptime(current_trackpoint['Date'], date_format)
                    continue
                if index == amount_of_trackpoints:
                    continue
                
                next_trackpoint = self.db['TrackPoint'].find_one({"_id":trackpoint_id})
                next_trackpoint_timestamp = datetime.strptime(next_trackpoint['Date'], date_format)
                
                if (next_trackpoint_timestamp - current_trackpoint_timestamp).total_seconds() / 60 > 5:
                    if user_id in invalid_users_dict:
                        invalid_users_dict[user_id] += 1
                    else:
                        invalid_users_dict[user_id] = 1
                        pprint(user_id)
                    
                current_trackpoint = next_trackpoint
                current_trackpoint_timestamp = datetime.strptime(next_trackpoint['Date'], date_format)
                
        pprint(invalid_users_dict)
        self.close()    
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

