
from first_task import create_collection


def get_data(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        items = []
        data = {}
        for line in file:
            line = line.strip()
            if line == "=====":
                items.append(data)
                data = {}
                continue
            split = line.split("::")
            key = split[0].strip()
            if key in ['salary', 'id', 'year', 'age']:
                value = int(split[1].strip()) if len(split) > 1 else None
            else:
                value = split[1].strip() if len(split) > 1 else None
            data[key.strip()] = value
        return items


def insert_data(collection):
    collection.insert_many(get_data('./58/task_3_item.text'))


client, collection = create_collection()
#insert_data(collection)

deleted = collection.delete_many({'$or': [{"salary": {'$lt': 25_000}}, {"salary": {'$gt': 175_000}}]})
print(deleted)

updated = collection.update_many({}, {'$inc': {'age': 1}})
print(updated)

updated = collection.update_many({'job': {'$in': ['Повар', 'Водитель']}},
                                 {'$mul': {'salary': 1.05}})
print(updated)

updated = collection.update_many({'city': {'$in': ['Алма-Ата', 'Алькала-де-Энарес']}},
                                 {'$mul': {'salary': 1.07}})
print(updated)

updated = collection.update_many({
    'job': {'$in': ['Бухгалтер', 'Инженер']},
    'city': 'Аликанте',
    '$or': [
        {'age': {'$gte': 35}},
        {'age': {'$lte': 20}}
    ]
}, {'$mul': {'salary': 1.1}})
print(updated)

deleted = collection.delete_many({'$or': [{"age": {'$lt': 20}}, {"salary": {'$gt': 150_000}}]})
print(deleted)
