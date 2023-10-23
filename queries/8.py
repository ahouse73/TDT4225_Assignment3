from pprint import pprint 
from DbConnector import DbConnector
from decouple import config

# Find the top 20 users who have gained the most altitude meters
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        
        pipeline = [{"$match":{"Altitude":{"$ne":-777}}},
                    {"$group":
                        {"_id":"$user_id",
                         "totalAlt":{"$sum":"$Altitude"}}},
                    {"$sort":{"totalAlt":-1}},{"$limit":20}]
        
        result = list(self.db['TrackPoint'].aggregate(pipeline))
        
        pprint(result)
        
        
        self.close()       
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

