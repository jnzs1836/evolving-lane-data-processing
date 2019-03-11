from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import BulkWriteError
import math

keys = ['shopping-convenience','public-service','domestic-helper','office-building','tourism-convenience']
def match(poi,collection):
    if poi['loc']['coordinates'][1] > poi['loc']['coordinates'][0]:
         return None
    query_doc = {"line-string":
        {'$near':
            {'$geometry': {
                   'type': "Point",
                   'coordinates': [ poi['loc']['coordinates'][0], poi['loc']['coordinates'][1]]
            },
    '$maxDistance': 75,
    '$minDistance': 0 }
        }
    }
    edges = collection.find(query_doc)
    for edge in edges:
        for key in keys:
            edge['poi-scores'][key] += poi['scores'][key]
        collection.update({'_id': edge['_id']}, edge, True)

    return edges

if __name__ == '__main__':
    conn = MongoClient()
    db = conn.hangzhou
    poi_collection = db.poi
    index_collection = db.inverted_index
    print("db loaded")
    poi_list = poi_collection.find()
    edges = index_collection.find()
    print("data loaded")
    count = 0
    for poi in poi_list:
        if count % 2000 == 0:
            print(str(count) + " is done")
        if match(poi, index_collection):
            count += 1

