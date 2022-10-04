import pymongo
from pprint import pprint
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
print("Connection Successful")

database = client.AssignmentMongoDB

new_movie = [{
    "plot": "Jujutsu Kaise",
    "genres": ["dark", "supernatural"],
    "title": "shut"
}]

new_comment = [{
    "name": "Ken Keneki",
    "text": "Searching For Someone To Blame Is Such A Pain.",
}]

new_theater = [{
    "theater_id": 8989,
    "location": {
        "address": {
            "city": "Prayagraj"
        }
    }
}]

new_user = [{
    "name": "Yuno",
    "email": "yuno@blackclover.world.com",
    "password": "asIfIWillTellYa"
}]


# task 3 - Inserting Data in respective collections
def insert_document(collection,new_doc):
    if collection == "comments":
        database.comments.insert_many(new_doc)
    elif collection == "movies":
        database.movies.insert_many(new_doc)
    elif collection == "sessions":
        database.sessions.insert_many(new_doc)
    elif collection == "theaters":
        database.theaters.insert_many(new_doc)
    elif collection == "users":
        database.users.insert_many(new_doc)

insert_document("comments",new_comment)
insert_document("movies",new_movie)
insert_document("theaters",new_theater)
insert_document("users",new_user)