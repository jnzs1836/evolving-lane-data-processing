from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import BulkWriteError
import math
from poi.category_process import get_category
def match(poi,collection,edge_poi_index_dict,keys):
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
    edge_index = {}
    edges = collection.find(query_doc)
    for edge in edges:
        for key in keys:
            if poi['scores'][key] > 0:
                edge_poi_index_dict[str(edge['id'])][key].append(poi['_id'])
                # edge_poi_index_dict[str(edge['id'])][key] = list(set(edge_poi_index_dict[str(edge['id'])][key]))
            edge['poi-scores'][key] += poi['scores'][key]
        collection.update({'_id': edge['_id']}, edge, True)

    return edge_poi_index_dict

if __name__ == '__main__':
    category = get_category('./data/poi_type/')
    keys = category.keys()
    conn = MongoClient()
    db = conn.hangzhou
    poi_collection = db.point_of_interest
    index_collection = db.inverted_index
    print("db loaded")
    poi_list = poi_collection.find()
    edges = index_collection.find()
    print("data loaded")
    edge_poi_index = db.edge_poi_index
    edge_poi_index.drop()
    edge_poi_dict = {}
    for edge in edges:
        edge['poi-scores'] = {}
        edge_poi = {"edge_id":edge['id']}

        for key in category.keys():
            edge['poi-scores'][key] = 0
            edge_poi[key] = []

        index_collection.update({'_id': edge['_id']}, edge, True)
        edge_poi_dict[str(edge['id'])] = edge_poi
    print("edges updated")


    print("index created")

    count = 0
    for poi in poi_list:
        count += 1
        if count % 5000 == 0:
            print(str(count) + " is done")
        edge_poi_dict=match(poi, index_collection,edge_poi_dict,keys)
    print(list(edge_poi_dict.values())[:9])
    edge_poi_index.insert_many(list(edge_poi_dict.values()))
