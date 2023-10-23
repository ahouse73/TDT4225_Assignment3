from pprint import pprint 
from DbConnector import DbConnector
from decouple import config

# Find the average number of activities per user.
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):        
        obj = {} # Empty object because filter argument is required
        act_amount = self.db['Activity'].count_documents(obj)
        user_amount = self.db['User'].count_documents(obj)
        
        print(f"Average amount of activities per user: {act_amount / user_amount}")
        
        self.close()
        
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

