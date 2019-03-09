from coordinates import gcj02towgs84
from pymongo import MongoClient, GEOSPHERE


if __name__ == '__main__':
    conn = MongoClient()
    db = conn.hangzhou
    poi_collection = db.poi
    poi_list = poi_collection.find()
    count = 0
    for poi in poi_list:
        count += 1
        if count % 2000 == 0:
            print(str(count) + " is done")
        poi['loc']['coordinates'] = gcj02towgs84(poi['loc']['coordinates'][1],poi['loc']['coordinates'][0])
        poi_collection.update({'_id': poi['_id']}, poi, True)