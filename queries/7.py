from pprint import pprint 
from DbConnector import DbConnector
from decouple import config
from datetime import datetime
from haversine import haversine
# Find the total distance (in km) walked in 2008, by user with id=112
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        
        activities = list(self.db['Activity'].find({"user_id":'112', 
                                                "transportation_label":"walk"}))
        
        total_distance_walked = 0
        for activity in activities:
            start_time = activity['start_time']
            end_time = activity['end_time']
            
            if (start_time.year != 2008) or (end_time.year != 2008):
                continue
            
            trackpoints = activity['trackpoints']
            
            for index, tp in enumerate(trackpoints):
                tp = self.db['TrackPoint'].find_one({"_id" : tp})
                
                if index == 0:
                    prev_lat = tp['Latitude']
                    prev_lon = tp['Longitude']
                else:
                    lat = tp['Latitude']
                    lon = tp['Longitude']
                    total_distance_walked += haversine((lat, lon), (prev_lat, prev_lon)) 
                    
                    prev_lat = lat
                    prev_lon = lon
        
        
        print(f"Total distance walked in km: {total_distance_walked}")
        self.close()       
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

