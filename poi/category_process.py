from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import BulkWriteError
import pandas as pd
import numpy as np
import os
def get_collection():
    conn = MongoClient()
    db = conn.hangzhou
    return db.point_of_interest

def map_type(poi,category):
    types = poi.get("types")
    scores = {}
    attr = []
    for name in category.keys():
        scores[name] = 0.
    my_types = []
    for type in types:
        contain_types = type.split('|')
        for my_type in contain_types:
            my_types.append(my_type)
    my_category = []
    for type_name in category.keys():
        for poi_type in my_types:
            if poi_type in category[type_name]:
                scores[type_name] = 1.0
                my_category.append(type_name)
                break
    poi['scores'] = scores
    poi['category'] = my_category
    # if len(my_category) == 0:
        # print(my_types)
    return poi

def process_poi_list(category):
    print("start processing")
    collection = get_collection()
    print("get db")
    pois = collection.find()
    count = 0
    print("retrieve all data")
    for poi in pois:
        poi = map_type(poi,category)
        if len(poi['category']) == 0:
            collection.remove({"_id":poi['_id']})
            continue
        collection.update({'_id':poi['_id']},poi,True)
        count+=1
        if count == 5000:
            print(str(count)+' is done')
    print(count)
def get_category(path='../data/poi_type/'):
    dir_path = path
    file_names = os.listdir(dir_path)
    my_category = {}
    for file_name in file_names:
        with open(dir_path+file_name,'r',encoding='utf-8') as file:
            # print(list(map(lambda x:x.strip(),file.readlines())))
            my_category[file_name[:-4]] = list(map(lambda x:x.strip(),file.readlines()))
            print(file_name[:-4])
            print(my_category[file_name[:-4]])
    return my_category

if __name__ == '__main__':
    type_category = get_category()
    process_poi_list(type_category)
    # data,text = prepare_process()
    # tmp = pca(data)
    # k = list(map(parse_component,tmp))
    # for i in k:
    #     s = list(map(lambda x:text[x],i))
    #     print(s)
    # print(text[tmp])
    # process_pois()
    # modify_coordinates(