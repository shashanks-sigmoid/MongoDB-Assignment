import json
from bson import ObjectId
from pymongo import MongoClient

# task1 - MongoDB connection
client = MongoClient("mongodb://127.0.0.1:27017")
print("Connection Successful")

# Assigning Database name
database = client.AssignmentMongoDB
# Collections name
collections_name = ["comments", "movies", "sessions", "theaters", "users"]

# task2 - Load each data set in respective collection.
dbnames = client.list_database_names()

if 'AssignmentMongoDB' not in dbnames:

    for collection in collections_name:

        item_list = []

        with open(f'mflix_data/{collection}.json') as f:

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

client.close()