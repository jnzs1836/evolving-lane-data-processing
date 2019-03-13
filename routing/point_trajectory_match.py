from pymongo import MongoClient

category = []
def match_trajectory(trajectory,collection ):
    query_doc = { 'loc' :
                         { '$geoWithin' :
                           { '$centerSphere' :
                              [  trajectory['origin']['coordinates']  , 0.310686/3963.2 ] }
                      }}
    points = collection.find(query_doc)
    trajectory['OriginCategory'] =[0] * 18
    for point in points:
        size = float(len(point['category']))
        for one_category in point['category']:
            trajectory['OriginCategory'][one_category] += 1./size
    query_doc = {'loc':
                     {'$geoWithin':
                          {'$centerSphere':
                               [trajectory['destination']['coordinates'], 0.310686 / 3963.2]}
                      }}
    points = collection.find(query_doc)
    trajectory['DestinationCategory'] = [0] * 18
    for point in points:
        size = float(len(point['category']))
        for one_category in point['category']:
            trajectory['DestinationCategory'][one_category] += 1. / size
    return trajectory

def test_one():
    trajectory = {}

def match():
    conn = MongoClient()
    db = conn.hangzhou
    # db.drop_collection('trajectory_score_index')
    point_collection = db.point_of_interest
    trajectory_collection = db.trajectory



