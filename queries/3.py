from pprint import pprint 
from DbConnector import DbConnector
from decouple import config

# Find the top 20 users with the highest number of activities.
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        
        pipeline = [{"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 20}]
        
        result = list(self.db['Activity'].aggregate(pipeline))
        
        pprint(result)
        
        self.close()    
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

