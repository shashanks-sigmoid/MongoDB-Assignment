import json
from bson import ObjectId
from pymongo import MongoClient
import os

# task1 - MongoDB connection
client = MongoClient("mongodb://127.0.0.1:27017")
print("Connection Successful")

# Assigning Database name
database = client.AssignmentMongoDB
# Collections name
collections_name = ["comments", "movies", "sessions", "theaters", "users"]

# task2 (method 1 - using python, mongoDB query) - Load each data set in respective collection.
dbnames = client.list_database_names()

if 'AssignmentMongoDB' not in dbnames:

    for collection in collections_name:

        item_list = []

        with open(f"./mflix_data/{collection}.json") as f:

            for json_obj in f:

                if json_obj:

                    my_dict = json.loads(json_obj)

                    my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])

                    item_list.append(my_dict)

        if collection == "comments":

            database.comments.insert_many(item_list)

        elif collection == "movies":

            database.movies.insert_many(item_list)

        elif collection == "sessions":

            database.sessions.insert_many(item_list)

        elif collection == "theaters":

            database.theaters.insert_many(item_list)

        elif collection == "users":

            database.users.insert_many(item_list)

# task2 (method 2 - using os, mongoimport cli command) - Load each data set in respective collection.

## comments collection creation
# os.system('mongoimport --db AssignmentMongoDB --collection comments --file mflix_data/comments.json')
## movies collection creation
# os.system('mongoimport --db AssignmentMongoDB --collection movies --file mflix_data/movies.json')
## theaters collection creation
# os.system('mongoimport --db AssignmentMongoDB --collection theaters --file mflix_data/theaters.json')
## users collection creation
# os.system('mongoimport --db AssignmentMongoDB --collection users --file mflix_data/users.json')
## sessions collection creation
# os.system('mongoimport --db AssignmentMongoDB --collection sessions --file mflix_data/sessions.json')


client.close()