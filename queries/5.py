from pprint import pprint 
from DbConnector import DbConnector
from decouple import config

# Find all types of transportation modes and count how many activities that are
# tagged with these transportation mode labels
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        
        pipeline = [{"$match":{"transportation_label":{"$ne":""}}},
                    {"$group" : {"_id":"$transportation_label", "count":{"$sum":1}}},
                    {"$sort":{"count":-1}}]
        
        result = list(self.db['Activity'].aggregate(pipeline))
        
        pprint(result)
        
        self.close()       
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

