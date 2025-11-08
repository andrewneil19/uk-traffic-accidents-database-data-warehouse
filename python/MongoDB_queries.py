import pymongo
from tabulate import tabulate
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# List the available databases
# print("Available databases:", client.list_database_names())

def find_query(dbName, collectionName, query, projection, sort_col):
    db = client[dbName]  # Specify the database name
    collection = db[collectionName]  # Specify the collection name
    query_output = collection.find(query, projection).sort(sort_col)
    documents = list(query_output)
    df = pd.DataFrame(documents)
    column_names = list(collection.find_one(query, projection).keys())
    result_table = tabulate(df, headers = column_names, tablefmt="grid", showindex="False")
    query_text = "db." + str(collectionName) + ".find(" + str(query) + ",\n" + str(projection) + ",\n" + str(sort_col) + ")"
    return result_table, query_text
#
def aggregate_query(dbName, collectionName, pipeline):
    db = client[dbName]  # Specify the database name
    collection = db[collectionName]  # Specify the collection name
    pipe_output = collection.aggregate(pipeline)
    first_document = next(pipe_output, None)  # Get the first document or None if no documents
    if first_document:
        column_names = list(first_document.keys())
    else:
        print("No documents found")
    documents = list(pipe_output)
    df = pd.DataFrame(documents)
    result_table = tabulate(df, headers = column_names, tablefmt="grid", showindex="False")
    agg_text = "db." + str(collectionName) + ".aggregate(" + str(pipeline) + ")"
    return result_table, agg_text
#
db = 'Term_Project_MSIS_5663'
collection_accidents = 'accidents_extract'
collection_vehicles = 'vehicles_extract'
#
#
# Execute a MongoDB NoSQL find() query
# Define the query criteria
# 1 - List of accidents where police had been at the scene
query1 = {"Did_Police_Officer_Attend_Scene_of_Accident": 1}

# 2- Show accident info but only where the road speed limit was greater than 50 and first road_class is “Motorway”
query2 = {"$and": [ {"Speed_limit": {"$gt": 50}}, {"1st_Road_Class": "Motorway"} ] }

# 3 - Find vehicles where the age of the vehicle was greater than or equal to 40, and the vehicle type
# was not car. Ordered by age of vehicle in descending order
query3 = {"$and": [ {"Age_of_Vehicle": {"$gte": 30}}, {"Vehicle_Type": {"$ne": "Car"}} ]}

# Define projections to include only certain fields
projection1 = {"Accident_Index": 1, "Date": 1, "Time": 1, "_id": 0}
projection2 = {"Accident_Index": 1, "Date": 1, "Time": 1, "_id": 0}
projection3 = {"Accident_Index": 1, "Age_of_Vehicle": 1, "Vehicle_Type": 1, "_id": 0}
# Specify Sorts
sort_col_1 = {"Date": 1}
sort_col_2 = {"Date": 1}
sort_col_3 = {"Age_of_Vehicle": -1}

# Call the query output function
# Query 1
result_table, query_text = find_query(db, collection_accidents, query1, projection1, sort_col_1)
#print(query_text)
#print(result_table)

# Query 2
result_table, query_text = find_query(db, collection_accidents, query2, projection2, sort_col_2)
#print(query_text)
#print(result_table)

# Query 3
result_table, query_text = find_query(db, collection_vehicles, query3, projection3, sort_col_3)
#print(query_text)
#print(result_table)

#
#
# Call aggregation pipelines

# Find total accident count for urban and rural areas, but now more detailed: for each year.
# Exclude unallocated area
pipeline1 = [
        {"$match": {"Urban_or_Rural_Area": {"$ne": "Unallocated"}}},
        {"$group": {
            "_id": {
                "Year": "$Year",
                "AreaType": "$Urban_or_Rural_Area"
            },
            "Accident Count": {"$sum": 1}
        }},
        {"$project": {"_id": 0, "Year": "$_id.Year", "AreaType": "$_id.AreaType",
        "AccidentCount": 1}},
        {"$sort": {"Year": 1, "AreaType": 1}}
    ]

# Show the average age of Volvo cars, displaying the make and model
pipeline2 = [
    {"$match": {"make": "VOLVO"}},
    {"$group": {
        "_id": {"make": "$make", "model": "$model"},
        "average_age": {"$avg": "$Age_of_Vehicle"}
    }},
    {"$project": {"_id": 0, "make": "$_id.make","model": "$_id.model",
        "AverageAge": {"$round": ["$average_age", 1]} }
    }
]

# Agg 1
result_table, agg_text  = aggregate_query(db, collection_accidents, pipeline1)
#print(agg_text)
#print(result_table)

# Agg 2
result_table, agg_text  = aggregate_query(db, collection_vehicles, pipeline2)
#print(agg_text)
print(result_table)

