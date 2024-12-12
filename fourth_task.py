import csv
import json
from datetime import datetime
import pymongo
from pymongo import MongoClient

from first_task import execute_query_and_save_result


def get_data_from_csv():
    with open('./58/RottenTomatoesMovies.csv', 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        movies = []
        reader.__next__()
        for row in reader:
            movie = {
                'movie_title': row[0],
                'movie_info': row[1],
                'critics_consensus': row[2],
                'rating': row[3],
                'genre': row[4],
                'directors': row[5],
                'writers': row[6],
                'cast': row[7],
                'in_theaters_date': datetime.strptime(row[8], '%Y-%m-%d') if row[8] != '' else None,
                'on_streaming_date': datetime.strptime(row[9], '%Y-%m-%d'),
                'runtime_in_minutes': float(row[10]) if row[10] != '' else None,
                'studio_name': row[11],
                'tomatometer_status': row[12],
                'tomatometer_rating': int(row[13]),
                'tomatometer_count': int(row[14]),
                'audience_rating': float(row[15]) if row[15] != '' else None,
                'audience_count': float(row[16]) if row[16] != '' else None,
            }
            movies.append(movie)
    return movies


def get_data_from_json():
    with open('./58/RottenTomatoesMovies.json', 'r', encoding='utf-8') as file:
        return json.load(file)

def insert_data(collection):
    collection.insert_many(data_from_csv)
    collection.insert_many(data_from_json)

data_from_csv = get_data_from_csv()
data_from_json = get_data_from_json()
client = MongoClient('localhost', 27017)
db = client['data_engineering']
collection = db.movies
insert_data(collection)

execute_query_and_save_result(lambda: collection.find({}, {'_id': False}, limit=10)
                              .sort([('audience_count', pymongo.DESCENDING)]),
                              './results/4/1.1_sort_by_audience_count.json')

execute_query_and_save_result(lambda: collection.find({
    'genre': {'$in': ['Comedy', 'Action']},
    'runtime_in_minutes': {'$ne': 'null'},
}, {'_id': False}, limit=10)
                              .sort([('runtime_in_minutes', pymongo.DESCENDING)]),
                              './results/4/1.2_comedies_and_actions_sorted_by_runtime_in_minute.json')

execute_query_and_save_result(lambda: collection.find({
    'tomatometer_status': 'Fresh',
    'genre': 'Drama'
}, {'_id': False}, limit=10)
                              .sort([('in_theaters_date', pymongo.DESCENDING)]),
                              './results/4/1.3_fresh_dramas_sorted_by_date_in_theaters.json')

execute_query_and_save_result(lambda: collection.find({
    'audience_rating': {'$gte': 70, '$lte': 100},

}, {'_id': False}, limit=10)
                              .sort([('audience_rating', pymongo.DESCENDING)]),
                              './results/4/1.4_movies_by_audience_rating_from_70_to_100.json')

execute_query_and_save_result(lambda: collection.find({
    'studio_name': '20th Century Fox'
}, {'_id': False}, limit=10)
                              .sort([('audience_count', pymongo.ASCENDING)]),
                              './results/4/1.5_lowest_20th_Century_Fox_movies_by_audience_count.json')

execute_query_and_save_result(
    lambda: collection.aggregate([{
        '$group': {
            '_id': 'null',
            'MaxAudienceRating': {'$max': '$audience_rating'},
            'MinAudienceRating': {'$min': '$audience_rating'},
            'AvgAudienceRating': {'$avg': '$audience_rating'},
        }
    }]),
    './results/4/2.1_audience_rating_characteristics.json')

execute_query_and_save_result(
    lambda: collection.aggregate([{
        '$group': {
            '_id': '$rating',
            'MaxAudienceRating': {'$max': '$audience_rating'},
            'MinAudienceRating': {'$min': '$audience_rating'},
            'AvgAudienceRating': {'$avg': '$audience_rating'},
        }
    }]),
    './results/4/2.2_audience_rating_characteristics_by_rating.json')

execute_query_and_save_result(
    lambda: collection.aggregate([
        {
            '$match': {
                "runtime_in_minutes": {'$gte': 120, '$lte': 180}
            }
        },
        {
            '$group': {
                '_id': 'null',
                'MaxAudienceRating': {'$max': '$audience_rating'},
                'MinAudienceRating': {'$min': '$audience_rating'},
                'AvgAudienceRating': {'$avg': '$audience_rating'},
            }
        }]),
    './results/4/2.3_audience_rating_characteristics__movies_with_runtime_between_2_and_3_hours.json')

execute_query_and_save_result(
    lambda: collection.aggregate([
        {
            '$group': {
                '_id': ['$directors', '$tomatometer_status'],
                'MaxAudienceRating': {'$max': '$audience_rating'},
                'MinAudienceRating': {'$min': '$audience_rating'},
                'AvgAudienceRating': {'$avg': '$audience_rating'},
            }
        }, {
            '$limit': 100
        }]),
    './results/4/2.4_audience_rating_characteristics_by_directors_and_status.json')

execute_query_and_save_result(
    lambda: collection.aggregate([
        {
            '$match': {
                "tomatometer_status": {'$in': ['Fresh', 'Certified Fresh', 'Rotten']}
            }

        },
        {
            '$group': {
                '_id': '$studio_name',
                'Count': {'$sum': 1},
                'MaxAudienceCount': {'$max': '$audience_count'},
                'MinAudienceCount': {'$min': '$audience_count'},
                'AvgAudienceCount': {'$avg': '$audience_count'},
            },
        },
        {
            '$sort': {
                'Count': pymongo.DESCENDING
            }
        }]),
    './results/4/2.5_audience_count_characteristics_by_studio_name_with_condition.json')

deleted = collection.delete_many({"directors": 'Chris Columbus'})
print(deleted)

updated = collection.update_many({
    'audience_count': {
        '$ne': None,
        '$exists': True
    }
}, {'$mul': {'audience_count': 10}})
print(updated)

updated = collection.update_many({
    'studio_name': 'Sony Pictures Classics'
}, [{'$set': {'directors': {'$concat': ["$directors", ",Chusov Egor"]}}}])
print(updated)

deleted = collection.delete_many({"genre": {'$regex': 'Drama'}})
print(deleted)

updated = collection.update_many({
    'movie_title': {'$regex': 'Harry Potter'}
},
    {'$inc': {'tomatometer_count': -30}})
print(updated)
