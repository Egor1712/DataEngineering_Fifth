import json

import pymongo
from pymongo import MongoClient


def create_collection():
    client = MongoClient('localhost', 27017)
    db = client['data_engineering']
    return (client, db.jobs)


def get_data():
    with open('./58/task_1_item.json', 'r+', encoding='utf-8') as f:
        return json.load(f)


def insert_data(collection):
    collection.insert_many(get_data())


def execute_query_and_save_result(query, filename, need_cast_to_list=True):
    results = list(query()) if need_cast_to_list else query()
    with open(f'{filename}', 'w+', encoding='utf-8') as file:
        json.dump(results, file, ensure_ascii=False, default=str)


client, collection = create_collection()
data = get_data()
if ('jobs' not in client['data_engineering'].list_collection_names()):
    insert_data(collection)

execute_query_and_save_result(
    lambda: collection.find({}, {'_id': False}, limit=10).sort([('salary', pymongo.DESCENDING)]),
    './results/1/first_task_sorted_by_salary.json')

execute_query_and_save_result(
    lambda: collection.find({
        'age': {'$lt': 30}
    },
        {'_id': False},
        limit=15)
    .sort([('salary', pymongo.DESCENDING)]),
    './results/1/first_task_sorted_by_salary_filter_by_age.json')

execute_query_and_save_result(
    lambda: collection.find({
        'city': 'Вильнюс',
        'job': {'$in': ['IT-специалист', 'Строитель', 'Архитектор']}
    },
        {'_id': False},
        limit=10)
    .sort([('age', pymongo.ASCENDING)]),
    './results/1/first_task_sorted_by_age_filter_by_city_and_jobs.json')

execute_query_and_save_result(
    lambda: collection.count_documents({
        'age': {'$gte': 45, '$lte': 60},
        'year': {'$in': [2019, 2022]},
        '$or': [
            {'salary': {'$gt': 50_000, '$lte': 75_000}},
            {'salary': {'$gt': 125_000, '$lt': 150_000}}
        ]
    }),
    './results/1/first_task_count_with_complex_filter.json', False)
