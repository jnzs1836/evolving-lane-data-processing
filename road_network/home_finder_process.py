from pymongo import MongoClient, GEOSPHERE
# This file processes the road network data used in HomeFinder.
FORMAT = [int, int, int, int, int, int, float, float, float, float, float]



def process_homefinder_data():
    conn = MongoClient()
    db = conn.bicycle_lane_planning
    db.drop_collection('road_network')
    collection = db.road_network

    print('reading roads...')
    fr = open('./Hangzhou/road_network.txt', 'r')
    ls = []
    for line in fr:
        line = line.strip()
        if len(line) == 0: break
        ls.append(list(map(lambda p: p[0](p[1]), zip(FORMAT, line.split(' ')))))

    print('inserting db...')
    data = []
    for l in ls:
        # 如果起点终点经纬度相同，则不是一条路了
        if l[8] == l[10] or l[7] == l[9]:
            l[10] += 0.00001
            l[9] += 0.00001
        data.append({
            'id': l[0],
            'sid': l[1],
            'snid': l[2],
            'dnid': l[3],
            'osid': l[4],
            'cid': l[5],
            'length': l[6],
            'loc': {
                'type': 'LineString',
                'coordinates': [ [ l[8], l[7] ], [ l[10], l[9] ] ]
            },
        })

    collection.insert_many(data, ordered=False)
    collection.ensure_index([('loc', GEOSPHERE)])

if __name__ == '__main__':
    process_homefinder_data()
