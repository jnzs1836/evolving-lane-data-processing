from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import BulkWriteError
# This file processes the fin_* data files.
FORMAT = [int, int, int, float]

def convert_edge(raw_id):
    return raw_id

def filter_data(le,lv):
    adjacent = set()
    for item in le:
        if item[1] == 0:
            adjacent.add(item[2])
        elif item[2] == 0:
            adjacent.add(item[1])
    nle= []
    nlv = []
    for item in le:
        if item[1] in adjacent:
            adjacent.add(item[2])
        elif item[2] in adjacent:
            adjacent.add(item[1])
    for item in adjacent:
        nlv.append(lv[item])
    for item in le:
        if item[1] in adjacent or item[2] in adjacent:
            nle.append(item)

    return nle, nlv


def process_fin_data():
    conn = MongoClient()
    db = conn.hangzhou
    db.drop_collection('edge')
    db.drop_collection('vertex')
    e_collection = db.edge

    v_collection = db.vertex

    print('reading roads...')

    fe = open('./Hangzhou/finRoadSegment.txt', 'r')
    fv = open('./Hangzhou/finNodes.txt', 'r')
    next(fe)
    next(fv)
    le = []
    lv = []
    data = []
    # print(len(fe))
    for line in fe:
        if len(line) == 0: break
        line = line.strip()
        tmp = line.split(" ")
        le.append(tmp)
    for line in fv:
        line = line.strip()
        if len(line) == 0: break
        tmp = line.split(" ")
        lv.append(tmp)
    a = le
    b = lv
    # a,b = filter_data(le,lv)
    for item in a:
        doc = {
            'id':convert_edge(int(item[0])),
            'vertex1':int(item[1]),
            'vertex2':int(item[2]),
            'length':float(item[3])
        }
        data.append(doc)
    print(len(data))
    # print(data)
    try:
        e_collection.insert_many(data)
    except BulkWriteError as exception:
        print(exception.details)
    data = []


    for item in b:
        doc = {
            'id':int(item[0]),
            'loc':{
                'type':'Point',
                'coordinates':[float(item[1]),float(item[2])]
            }
        }
        data.append(doc)
    print(len(data))
    # print(data)
    print('inserting to edge db...')
    print('inserted to edge db...')
    v_collection.insert_many(data, ordered=False)
    v_collection.ensure_index([('coordinates', GEOSPHERE)])

if __name__ == '__main__':
    process_fin_data()
