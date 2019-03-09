from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import BulkWriteError
import math
FORMAT = [int, int, int, float]

INTERVAL_THRESHOLD = 5
def calculate_distance(coordinates1,coordinates2):
    m_latitude1 = math.pi*(90 - coordinates1[0])/180.0
    m_latitude2 = math.pi*(90 - coordinates2[0])/180.0
    m_longitude1 = math.pi*(coordinates1[1])/180.0
    m_longitude2 = math.pi*(coordinates2[1])/180.0
    c = math.sin(m_latitude1)*math.sin(m_latitude2)*math.cos(m_longitude1 - m_longitude2) + math.cos(m_latitude2)*math.cos(m_latitude1)
    r = 6371 * 10^3
    if c > 1:
        print(math.cos(m_latitude2)*math.cos(m_latitude1))
        print(math.sin(m_latitude1)*math.sin(m_latitude2))
        print(math.cos(m_latitude1 - m_latitude2))
        print(math.sin(m_latitude1))
        print(coordinates1[0])
        print(coordinates2[0])
        print(c)
        return 0
    return r * math.acos(c)*math.pi/180.0
def recursive_interpolation(point1,point2):
    get = []
    distance = calculate_distance(point1['loc']['coordinates'],point2['loc']['coordinates'])
    if distance <INTERVAL_THRESHOLD:
        return []
    else:
        mid_point_coordinates = [
            (point1['loc']['coordinates'][0] + point2['loc']['coordinates'][0])/2.0,
            (point1['loc']['coordinates'][1] + point2['loc']['coordinates'][1]) / 2.0
        ]
        mid_point = {
            'loc':
                {
                    'type': 'Point',
                    'coordinates':mid_point_coordinates
                }
        }
        get.extend(recursive_interpolation(point1,mid_point))
        get.append(mid_point)
        get.extend(recursive_interpolation(mid_point,point2))
        return get
def interpolation(through_points):
    head = through_points[0]
    get = []
    for point in through_points[1:]:
        get.append(head)
        distance = calculate_distance(head['loc']['coordinates'],point['loc']['coordinates'])

        if distance < INTERVAL_THRESHOLD:
            get.extend(recursive_interpolation(head,point))
        else:
            pass
        head = point
    get.append(through_points[-1])
    return get


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
                    'loc': {
                        'type': 'Point',
                        'coordinates':edge_point_coordinate
                    }
                }
                edge_point_list.append(point)
            if count < num_of_nodes + 30:

                print(edge_point_list)
                print(interpolation(edge_point_list))
                print("__________________________________" + str(count - num_of_nodes))
            edge_points_lists.append(interpolation(edge_point_list))
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
    # a,b = filter_data(le,lv)
    for item in a:
        doc = {
            'id':convert_edge(int(item[0])),
            'vertex1':int(item[1]),
            'vertex2':int(item[2]),
            'length':float(item[3]),
            'through-points':edge_points_lists[it]
        }
        it += 1
        data.append(doc)


    conn = MongoClient()
    db = conn.hangzhou
    db.drop_collection('edge')
    db.drop_collection('vertex')
    e_collection = db.edge

    v_collection = db.vertex
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
    process_msra_data('./Hangzhou/')