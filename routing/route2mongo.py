from pymongo import MongoClient
import json
import os
from routing.point_trajectory_match import match_trajectory

dir_path = './data/output2/'


def parse_route_file(route,count,my_point_collection):
    id=0
    data = route['Route']
    for key in data.keys():
        id = key
    if 'message' in data[str(id)].keys():
        return {'id':-1}
    edge_id_list = data[str(id)]['paths'][0]['details']['edge_id']
    edge_documents = []
    edge_id = -1
    route['origin'] = {
        'coordinates': [route['DepartLongitude'],route['DepartLatitude']],
        'type':'Point'
    }
    route['destination'] = {
        'coordinates':[route['ArriveLongitude'],route['ArriveLatitude']],
        'type': 'Point'

    }
    if count == 0:
        print(edge_id_list)
    for edge in edge_id_list:
        if edge[2] == edge_id:
            continue
        edge_id = edge[2]
        edge_document = {
            "edge-id":edge_id,
            'count':0,
            'length':0
        }
        edge_documents.append(edge_document)
    route['edges'] = edge_documents
    route['id'] = count
    route = match_trajectory(route,my_point_collection)
    del route['Route']
    del route['DepartLongitude']
    del route['DepartLatitude']
    del route['ArriveLongitude']
    del route['ArriveLatitude']

    return route


def route_file2mongo(file_name,count,collection,my_point_collection):
    documents = []
    with open(file_name,'r') as file:
        print("loaded")
        data = json.load(file)
        for trajectory in data:
            document = parse_route_file(trajectory,count,my_point_collection)
            if document['id'] < 0:
                continue
            count+=1
            documents.append(document)
    print(documents[0])
    collection.insert_many(documents)
    return count


def route_files2mongo():
    count = 0
    conn = MongoClient()
    db = conn.hangzhou
    # db.drop_collection('trajectory')
    collection = db.trajectory
    point_collection = db.point_of_interest
    file_list = os.listdir(dir_path)
    for file in file_list:
        print("parse file " + dir_path + file +" at " + str(count) )
        count = route_file2mongo(dir_path + file,count,collection)
        print(count)

if __name__ == '__main__':
    #count = process_trajectory_data(0)
    route_files2mongo()
