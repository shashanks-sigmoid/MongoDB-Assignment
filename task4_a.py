from pymongo import MongoClient

# Task-4a - Queries for comment collection

# Task-4a(a) - Find top 10 users who made the maximum number of comments.
def task_one(comments):
    pipeline = [
        {'$group': {'_id': '$name','total': {'$sum': 1}}}
        ,{'$sort': {'total': -1}}
        ,{'$limit': 10}
    ]

    ans = comments.aggregate(pipeline)
    usernames = []
    for i in ans:
        usernames.append(i['_id'])
    return usernames

# Task-4a(b) - Find top 10 movies with most comments
def task_two(comments):

    # $lookup is used to join a document from one collection to a document of another collection of the same database based on some queries.
    # $unwind operator is used to deconstruct an array field in a document and create separate output documents for each item in the array.
    
    pipeline2 = [
        {'$group': {'_id': '$movie_id','total': {'$sum': 1}}}
        ,{'$sort': {'total': -1}}
        ,{'$limit': 10}
        ,{'$lookup': {'from': 'movies','localField': '_id','foreignField': '_id','as': 'data'}}
        ,{'$unwind': {'path': '$data','preserveNullAndEmptyArrays': False}}
        ,{'$project': {'data.title': 1}}
    ]

    new_data = comments.aggregate(pipeline2)
    movies_name = []
    for i in new_data:
        movies_name.append(i['data']['title'])
    return movies_name

# Task-4a(c) - Given a year find the total number of comments created each month in that year.
def task_three(comments,year):
    pipeline = [
        {"$group": {"_id": {"year": {"$year": "$date"},"month": {"$month": "$date"}},"total_person": {"$sum": 1}}}
        ,{"$match": {"_id.year": {"$eq": year}}}
        ,{"$sort": {"_id.month": 1}}
    ]
    result = comments.aggregate(pipeline)
    li = []
    for i in result:
        li.append(i)
    return li


def queries(comments):
    print('Find top 10 users who made the maximum number of comments')
    taskOne = task_one(comments)
    print(taskOne)

    print('Find top 10 movies with most comments')
    taskTwo = task_two(comments)
    print(taskTwo)

    print("All comments with given year i.e. 2000")
    year = 2000
    taskThree = task_three(comments, year)
    print(taskThree)


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017")
    print("Connection Successful")

    # collection
    comments = client.AssignmentMongoDB.comments
    queries(comments)