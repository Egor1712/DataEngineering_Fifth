import pickle

import pymongo

from first_task import create_collection, execute_query_and_save_result


def get_data():
    with open('./58/task_2_item.pkl', 'rb+') as file:
        return pickle.load(file)


def insert_data(collection):
    collection.insert_many(get_data())


client, collection = create_collection()
# insert_data(collection)

execute_query_and_save_result(
    lambda: collection.aggregate([{
        '$group': {
            '_id': 'null',
            'MaxSalary': {'$max': '$salary'},
            'MinSalary': {'$min': '$salary'},
            'AvgSalary': {'$avg': '$salary'},
        }
    }]),
    './results/2/salary_characteristics.json')

execute_query_and_save_result(
    lambda: collection.aggregate([{
        '$group': {
            '_id': '$city',
            'MaxSalary': {'$max': '$salary'},
            'MinSalary': {'$min': '$salary'},
            'AvgSalary': {'$avg': '$salary'},
        }
    }]),
    './results/2/salary_by_city_characteristics.json')

execute_query_and_save_result(
    lambda: collection.aggregate([{
        '$group': {
            '_id': '$job',
            'MaxSalary': {'$max': '$salary'},
            'MinSalary': {'$min': '$salary'},
            'AvgSalary': {'$avg': '$salary'},
        }
    }]),
    './results/2/salary_by_profession_characteristics.json')

execute_query_and_save_result(
    lambda: collection.aggregate([{
        '$group': {
            '_id': '$city',
            'MaxAge': {'$max': '$age'},
            'MinAge': {'$min': '$age'},
            'AvgAge': {'$avg': '$age'},
        }
    }]),
    './results/2/age_by_city_characteristics.json')

execute_query_and_save_result(
    lambda: collection.aggregate([{
        '$group': {
            '_id': '$job',
            'MaxAge': {'$max': '$age'},
            'MinAge': {'$min': '$age'},
            'AvgAge': {'$avg': '$age'},
        }
    }]),
    './results/2/age_by_profession_characteristics.json')

execute_query_and_save_result(
    lambda: collection.aggregate([
        {
            '$group': {
                '_id': '$age',
                'MaxSalary': {'$max': '$salary'},
            }
        },
        {
            '$sort': {
                '_id': pymongo.ASCENDING
            }
        },
        {
            '$limit': 1
        }
    ]),
    './results/2/max_salary_by_min_age.json')

execute_query_and_save_result(
    lambda: collection.aggregate([
        {
            '$group': {
                '_id': '$age',
                'MinSalary': {'$min': '$salary'},
            }
        },
        {
            '$sort': {
                '_id': pymongo.DESCENDING
            }
        },
        {
            '$limit': 1
        }
    ]),
    './results/2/min_salary_by_max_age.json')
