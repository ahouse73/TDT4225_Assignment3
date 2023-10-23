from pprint import pprint 
from DbConnector import DbConnector
from decouple import config


# How many users, activities and trackpoints are there in the dataset
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        obj = {} # Empty object because filter argument is required
        act_amount = self.db['Activity'].count_documents(obj)
        track_amount = self.db['TrackPoint'].count_documents(obj)
        user_amount = self.db['User'].count_documents(obj)
        
        print(f"Amount of Activities {act_amount}")
        print(f"Amount of TrackPoints {track_amount}")
        print(f"Amount of Users {user_amount}")
        
        self.close()
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

