from pprint import pprint 
from DbConnector import DbConnector
from decouple import config
from datetime import datetime

# a) Find the year with the most activities.
# b) Is this also the year with most recorded hours?
class Query:

    def __init__(self):
        self.connection = DbConnector(PASSWORD=config('USERPWD'))
        self.client = self.connection.client
        self.db = self.connection.db
        
    def perform_query(self):
        
        # a)
        pipeline_a = [{"$project": {"year": {"$year": "$start_time"}}},
                    {"$group": {"_id": "$year", "count": {"$sum": 1}}},
                    {"$limit": 1}]
        
        result = list(self.db['Activity'].aggregate(pipeline_a))
        year_most_activities = result[0]['_id']
        pprint(f"The year with the most activities: {year_most_activities}")
        
        # b)
        #    i) Get all years
        #    ii) For each year, get all activities
        #    iii) Calculate hours
        
        
        # i)
        pipeline_b= [{"$project":{"year":{"$year":"$start_time"}}},
                     {"$group":{"_id":"null","years":{"$addToSet":"$year"}}}]     
           
        years = list(self.db['Activity'].aggregate(pipeline_b))[0]["years"]
        years.sort()
        
        # ii)
        recorded_hours = {}
        for year in years:
            recorded_hours[year] = 0
            
            start_of_year = datetime(year, 1,1)
            end_of_year = datetime(year, 12, 31, 23, 59, 59)
            
            activities = self.db['Activity'].find({"start_time": {"$gte": start_of_year,"$lt": end_of_year}})
            
            # iii)
            for act in activities:
                start_time = act['start_time']
                end_time = act['end_time']
                
                recorded_hours[year] += (end_time - start_time).total_seconds() / 3600
        
        
        year_with_most_recorded_hours = max(recorded_hours, key=recorded_hours.get)
        pprint(f"The year with the most recorded hours: {year_with_most_recorded_hours}")
        
        self.close()       
    
    def close(self):
        self.connection.close_connection()

if __name__ == '__main__':
    result = Query().perform_query()

