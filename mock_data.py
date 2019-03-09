import random
from pymongo import MongoClient,GEOSPHERE
def process_mock_data():
    linked_list = []
    edge_list = []
    vertex_num = 50
    edge_num = 40
    vertexes = []
    edges = []
    edge_id = 0
    trajectory_size = 14
    for i in range(vertex_num):
        linked_list.append([])
        edge_list.append([])
        doc = {
            'id': i,
            'loc': {
                'type': 'Point',
                'coordinates': [random.random()+30,random.random()+120]
            }
        }
        for j in range(i+1,vertex_num):
            if random.random() <0.2:
                linked_list[i].append(j)
                edge_list[i].append(edge_id)
                edge_doc = {
                    'id': edge_id,
                    'vertex1': i,
                    'vertex2': j,
                    'length': random.random()*40
                }
                edge_id += 1
                edges.append(edge_doc)
        vertexes.append(doc)
    trajectories = []

    for i in range(trajectory_size):
        start_vertex = int(random.random()*vertex_num)
        current_vertex = start_vertex
        trajectory = {}
        trajectory['id'] = i
        trajectory['edges'] = []
        edge = {
            'edge-id': current_vertex,
            'count': 0,
            'length': 0.0
        }
        trajectory['edges'].append(edge)
        while True:
            if len(linked_list[current_vertex]) == 0:
                break
            index = int(random.random()*len(linked_list[current_vertex]))
            if random.random()<0.1:
                break
            edge = {
                'edge-id': edge_list[current_vertex][index],
                'count': 0,
                'length': 0.0
            }
            trajectory['edges'].append(edge)
            current_vertex = linked_list[current_vertex][index]
        trajectories.append(trajectory)
    print(trajectories)
    print(edges)
    print(vertexes)
    conn = MongoClient()
    db = conn.hangzhou
    db.drop_collection('edge')
    db.drop_collection('vertex')
    db.drop_collection('trajectory_score_index')
    t_collection = db.trajectory_score_index
    e_collection = db.edge
    v_collection = db.vertex
    e_collection.insert_many(edges)
    v_collection.insert_many(vertexes,ordered=False)
    v_collection.ensure_index([('coordinates', GEOSPHERE)])
    t_collection.insert_many(trajectories)