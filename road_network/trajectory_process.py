from pymongo import MongoClient
from road_network.fin_process import convert_edge
# This file processes data from map-matched trajectory data.
def save_mongo(trajectory,collection):
    pass
def process_trajectory_data(data_path):
    path = data_path + 'MapMatched 3-4 smaple/edge201603/1077.txt'
    conn = MongoClient()
    db = conn.hangzhou
    db.drop_collection('trajectory_score_index')
    collection = db.trajectory_score_index
    f = open(path,'r')
    seq = "#"
    data = []
    trajectory_id = 0
    trajectory = {}
    for line in f:
        content = line.strip().split(",")
        if content[0] == seq:
            print("new line")
            if trajectory_id > 10:
                break
            if trajectory_id != 0:
                collection.insert_one(trajectory)
            trajectory = {}
            trajectory['id'] = trajectory_id
            trajectory['edges'] = []
            trajectory_id += 1
        else:
            edge_id = int (content[2])
            edge = {
                'edge-id':convert_edge(edge_id),
                'count':0,
                'length':0.0
            }
            trajectory['edges'].append(edge)
    collection.insert_one(trajectory)
if __name__ == '__main__':
    process_trajectory_data('./Hangzhou/')