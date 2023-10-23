from pprint import pprint 
from DbConnector import DbConnector
from decouple import config

# Find the users who have tracked an activity in the Forbidden City of Beijing.
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        result = self.db['TrackPoint'].distinct("user_id",
            {"Latitude": { "$gte": 39.916 , "$lt": 39.917 },
             "Longitude":{ "$gte": 116.397, "$lt": 116.398 }})
        
        pprint(result)
        
        self.close()       
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

