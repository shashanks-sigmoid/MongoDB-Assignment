from pymongo import MongoClient

# Task-4c - Queries for theaters collection

# Task-4c(a) - Top 10 cities with the maximum number of theatres
def task_one(theaters):
    pipeline = [
        {"$group": {"_id": {"city": "$location.address.city"}, "total_theaters": {"$sum": 1}}},
        {"$sort": {"total_theaters": -1}},
        {"$limit": 10},
        {"$project": {"city_name": "$_id.city", "_id": 0, "total_theaters": 1}}
    ]
    li = theaters.aggregate(pipeline)
    res = []
    for i in li:
        res.append(f"City - {i['city_name']} ; Total Theaters - {i['total_theaters']}")
    return res

# Task-4c(b) - Top 10 theatres nearby given coordinates
def task_two_2(theaters, co_ordinates):
    pipeline2 = [
        {"$geoNear": { "near": {"type": "Point", "coordinates": co_ordinates},"maxDistance": 10000000, "distanceField": "distance"}}
        ,{"$project": {"city": "$location.address.city", "_id": 0, "theaterId":1,"distance":1}}
        ,{"$limit": 10}
    ]

    results2 = theaters.aggregate(pipeline2)
    res = []
    for result in results2:
        res.append(f"City - {result['city']} ; TheaterId - {result['theaterId']} ; Distance - {result['distance']}")

    return res

def queries(theaters):
    print('top 10 cities with max. of theaters')
    taskOne = task_one(theaters)
    print(taskOne)

    print("top 10 theatres nearby given coordinates")
    co_ordinates = [-85.76461, 38.327175]

    taskTwo = task_two_2(theaters,co_ordinates)
    print(taskTwo)


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017")

    # collection
    theaters = client.AssignmentMongoDB.theaters
    
    # run below query in mongo shell to make indexing on location.geo in theaters collection
    # db.theaters.createIndex({ "location.geo" : "2dsphere" }) 
    queries(theaters)