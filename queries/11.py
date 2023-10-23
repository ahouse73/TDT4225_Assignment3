from pprint import pprint 
from DbConnector import DbConnector
from decouple import config

# Find all users who have registered transportation_mode and their most used transportation_mode
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        
        pipeline = [{"$match":{"transportation_label":{"$ne":""}}},
                    {"$group":{"_id":{"user_id":"$user_id",
                                      "transportation_label":"$transportation_label"},
                               "count":{"$sum":1}}},
                    {"$sort":{"_id.user_id":1,"count":-1}}]
        
        result = list(self.db["Activity"].aggregate(pipeline))
        
        users = {}
        importance = ['walk', 'run', 'bike', 'bus', 'subway', 'taxi', 'car', 'boat', 'airplane'] # Order of importance (left more important)
        for obj in result:   
            user = (obj.get('_id')).get('user_id')
            transportation_mode = (obj.get('_id')).get('transportation_label')
            amount = obj.get('count')
            
            if not (user in users):
                users[user] = (transportation_mode, amount)
            else:
                other_mode, other_amount = users[user]
                
                if other_mode == transportation_mode:
                    if other_amount < amount:
                        users[user] = (transportation_mode, amount)
                    
                    if other_amount == amount:
                        other_import = importance.index(other_amount)
                        this_import = importance.index(transportation_mode)
                        
                        # Left is more important
                        if this_import < other_import:
                            users[user] = (transportation_mode, amount)
                        
        
        for user_id in users:
            mode, _ = users[user_id]
            print(user_id, mode)
                
        self.close()       
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

