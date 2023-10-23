from pprint import pprint 
from DbConnector import DbConnector
from decouple import config

# Find all users who have taken a taxi.
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        result = self.db['Activity'].distinct("user_id", {"transportation_label": "taxi"})
        pprint(result)
        
        self.close()    
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

