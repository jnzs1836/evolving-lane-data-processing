import gensim
from pymongo import MongoClient
from road_network.msra_prosess import convert_edge
import torch

torch.manual_seed(1)
# TRAJECTORY_SIZE = 4
EDGE_SIZE = 244234
def read_trajectory_file(data_path):
    path = data_path + 'MapMatched 3-4 smaple/edge201603/1077.txt'
    f = open(path, 'r')
    seq = "#"
    data = []
    trajectory_id = 0
    trajectory_set = []
    trajectory = {}
    for line in f:
        content = line.strip().split(",")
        if content[0] == seq:
            trajectory_set.append(trajectory)
            trajectory = {}
            trajectory['id'] = trajectory_id
            trajectory['edges'] = []
            trajectory_id += 1
        else:
            edge_id = int(content[2])
            edge = {
                'edge-id': convert_edge(edge_id),
                'count': 0,
                'length': 0.0
            }
            trajectory['edges'].append(edge)
    return trajectory_set[1:]

def read_db(collection):
    trajectory_set = collection.find()
    return trajectory_set
def parse_trajectory_set(trajectory_set):
    data = []
    for trajectory in trajectory_set:
        data.append(list(map(lambda x:'s'+str(x['edge-id']),trajectory['edges'])))
    return data
def embedding(data_path='./Hangzhou/'):
    conn = MongoClient()
    db = conn.hangzhou
    collection = db.trajectory_score_info
    trajectory_set = read_db(collection)
    training_data = parse_trajectory_set(trajectory_set)
    print(training_data[0:3])
    model = gensim.models.word2vec
    model = gensim.models.Word2Vec(training_data)
    print(model.most_similar(training_data[0][0]))
    model.wv.save_word2vec_format("./model.bin",binary=True)