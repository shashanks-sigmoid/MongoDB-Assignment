# MongoDB-Assignment
Sigmoid MongoDB Assignment

## Problem Statement 
1. Create a Python application to connect to MongoDB.

2. Bulk load the JSON files in the individual MongoDB collections using Python. MongoDB collections -
    1. comments
    2. movies
    3. theaters
    4. users

3. Create Python methods and MongoDB queries to insert new comments, movies, theatres, and users into respective MongoDB collections.

4. Create Python methods and MongoDB queries to support the below operations -
    1. comments collection
        1. Find top 10 users who made the maximum number of comments
        2. Find top 10 movies with most comments
        3. Given a year find the total number of comments created each month in that year

    2. movies collection
        1. Find top `N` movies - 
        2. with the highest IMDB rating
        3. with the highest IMDB rating in a given year
        4. with highest IMDB rating with number of votes > 1000
        5. with title matching a given pattern sorted by highest tomatoes ratings
    3. Find top `N` directors -
        1. who created the maximum number of movies
        2. who created the maximum number of movies in a given year
        3. who created the maximum number of movies for a given genre
    4. Find top `N` actors - 
        1. who starred in the maximum number of movies
        2. who starred in the maximum number of movies in a given year
        3. who starred in the maximum number of movies for a given genre
    5. Find top `N` movies for each genre with the highest IMDB rating

5. theatre collection
    1. Top 10 cities with the maximum number of theatres
    2. top 10 theatres nearby given coordinates

## Technologies and Packages Used
* MongoDB
* Python
* Studio3T
* pymongo

Created Virtual environment and put all the packages/modules required in requirement.txt and then install it. Command to make and activate environement is written below.
```
python3 -m venv env-name
source env-name/bin/activate
pip install requirement.txt
```


## Task 1 - [Solution](task1_and_task2.py)

Python code to setup MongoDB connection.
``` 
client = MongoClient("mongodb://127.0.0.1:27017")
```
Python code to close MongoDB connection
```
cient.close()
```

## Task 2 - [Solution](task1_and_task2.py)

I mentioned two method to load bulk data, first is using MongoClient in python and second is using mongoimport in python using os.system . I used former one. But mention second method in comment. Click on solution link to see the code.


## Task 3 - [Solution](task3.py)

Created insert_document function, which take two parameter first is collection name and second is new document. And add the new document in the given collection.
```
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
```


## Task 4 - 
### a) comments collection queries - [solution](task4_a.py)
* ``` 
  pipeline = [
        {'$group': {'_id': '$name','total': {'$sum': 1}}}
        ,{'$sort': {'total': -1}}
        ,{'$limit': 10}
    ]
    ```

* ``` 
    $lookup is used to join a document from one collection to a document of another collection of the same database based on some queries.
    $unwind operator is used to deconstruct an array field in a document and create separate output documents for each item in the array.
    
    pipeline2 = [
        {'$group': {'_id': '$movie_id','total': {'$sum': 1}}}
        ,{'$sort': {'total': -1}}
        ,{'$limit': 10}
        ,{'$lookup': {'from': 'movies','localField': '_id','foreignField': '_id','as': 'data'}}
        ,{'$unwind': {'path': '$data','preserveNullAndEmptyArrays': False}}
        ,{'$project': {'data.title': 1}}
    ]
    ```


### b) movies collection queries - [solution](task4_b.py)
* ```
  pipeline1 = [
        {'$project': {'title': '$title', 'rating': '$imdb.rating'}},
        {'$match': {'rating': {'$exists': True, '$ne': ''}}},
        {'$group': {'_id': {'rating': '$rating', 'title': '$title'}}},
        {'$sort': {'_id.rating': -1}},
        {'$limit': n}
    ]
    ```
* ```
  pipeline2 = [
        {"$match": {"year": year}},
        {"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n}
    ]
    ```
* ```
  pipeline3 = [
        {"$match": {"imdb.votes": {"$gt": "1000"}}},
        {"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n}
    ]
    ```
* ```
  pipeline4 = [
        {"$match": {"title": {"$regex": pattern}}},
        {"$project": {"_id": 0, "title": 1, "tomatoes.viewer.rating": 1}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": n}
    ]
    ```
* ```
  pipeline5 = [
        {"$unwind": "$directors"},
        {"$group": {"_id": {"director_name": "$directors"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$limit": n}
    ]
    ```
* ```
  pipeline6 = [
        {"$unwind": "$directors"},
        {"$group": {"_id": {"directors": "$directors", "year": "$year"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.year": year}},
        {"$project": {"_id.directors": 1, "no_of_films": 1}},
        {"$limit": n}
    ]
    ```
* ```
  pipeline7 = [
        {"$unwind": "$directors"},
        {"$unwind": "$genres"},
        {"$group": {"_id": {"directors": "$directors", "genres": "$genres"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.genres": genres}},
        {"$limit": n}
    ]
    ```
* ```
  pipeline8 = [
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$limit": n}
    ]
    ```
* ```
  pipeline9 = [
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast", "year": "$year"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.year": year}},
        {"$project": {"_id.year": 0}},
        {"$limit": n}
    ]
    ```
* ```
  pipeline10 = [
        {"$unwind": "$cast"},
        {"$unwind": "$genres"},
        {"$group": {"_id": {"cast": "$cast", "genres": "$genres"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.genres": genres}},
        {"$project": {"_id.genres": 0}},
        {"$limit": n}
    ]
    ```
* ```
  pipeline11 = [
        [{"$unwind": "$genres"}
        ,{"$match": { 'imdb.rating': {'$exists': 1, '$ne': ''}}}
        ,{'$group': {"_id":"$genres","title":{"$push":"$title"},"rating":{"$push":"$rating"}}}
        ,{"$project": {"_id": 0, "genre":"$_id.genres", "title":{"$slice": ['$title',0,n]}, 'rating':{"$slice": [ '$rating',0,n]}}}
        ,{"$sort": {"rating": -1,"_id.genres":1}} ]
    ]
    ```
### c) theaters collection queries - [solution](task4_c.py)

* ```
  pipeline = [
        {"$group": {"_id": {"city": "$location.address.city"}, "total_theaters": {"$sum": 1}}},
        {"$sort": {"total_theaters": -1}},
        {"$limit": 10},
        {"$project": {"city_name": "$_id.city", "_id": 0, "total_theaters": 1}}
    ]
    ```
* For 2nd query first we create index on location.geo to increase our access on co-ordinate points in document using below code and run the pipeline.
  ```
  In mongo-shell
  db.theaters.createIndex({ "location.geo" : "2dsphere" })

  In python code
  pipeline2 = [
        {"$geoNear": { "near": {"type": "Point", "coordinates": co_ordinates},"maxDistance": 10000000, "distanceField": "distance"}}
        ,{"$project": {"city": "$location.address.city", "_id": 0, "theaterId":1,"distance":1}}
        ,{"$limit": 10}
    ]
    ```