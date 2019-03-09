from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import BulkWriteError
import math
FORMAT = [int, int, int, float]
import json

def convert_edge(raw_id):
    return raw_id

def process_msra_data(data_path):
    num_of_nodes = 183749


    print('reading roads...')

    fe = open( data_path + 'Road_Network_HZ_2016Q1.txt', 'r',encoding='utf-8')
    next(fe)
    # next(fv)
    le = []
    lv = []
    data = []
    # print(len(fe))
    count = 0
    edge_points_lists = []
    for line in fe:
        if count > num_of_nodes:
            line = line.strip()
            tmp = line.split("\t")
            le.append(tmp)
            count += 1
            line = next(fe)
            edge_points = line.strip().split(",")
            edge_point_list = []
            for edge_point in edge_points:
                edge_point_coordinate = list(map(lambda x:float(x),edge_point.split(" ")))
                if count < num_of_nodes + 30:
                    # print(edge_point_coordinate)
                    pass
                point = {
                    'geometry': {
                        'type': 'Point',
                        'coordinates':edge_point_coordinate
                    }
                }
                edge_point_list.append(point)
            edge_points_lists.append(edge_point_list)
            if count <num_of_nodes +  30:
                print(tmp)
        elif count == num_of_nodes:
            count += 1
        else:
            line = line.strip()
            tmp = line.split("\t")
            lv.append(tmp)
            count += 1
            if count < 4:
                print(tmp)

    a = le
    b = lv
    it =  0
    edges = []
    # a,b = filter_data(le,lv)
    for item in a:
        doc = {
            "type": "Feature",
            "properties":{

            },
            'geometry':{
              'type':'LineString',
                'coordinates':edge_points_lists[it]
            },
            # 'vertex1':int(item[1]),
            # 'vertex2':int(item[2]),
            # 'length':float(item[3]),
        }
        it += 1
        edges.append(doc)


    points = []
    for item in b:
        doc = {
            "type": "Feature",
            "properties":{

            },
            'geometry':{
                'type':'Point',
                'coordinates':[float(item[1]),float(item[2])]
            }
        }
        points.append(doc)
    features = []

    features.extend(points)
    features.extend(edges)
    print(len(data))
    # print(data)
    print(points[0])
    print(edges[0])
    geo_json = {
        "type": "FeatureCollection",
        "generator": "JOSM",
        "bbox": [
            119.91580000000,
            30.10000000000,
            120.40120000000,
            30.47290000000
        ],
        'features':features
    }
    with open('./data/hangzhou.json','w') as out:
        json.dump(geo_json,out)

if __name__ == '__main__':
    process_msra_data('./data/Hangzhou/')