from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import BulkWriteError
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
def get_collection():
    conn = MongoClient()
    db = conn.hangzhou
    return db.poi

type_list_public_service = [u'公共设施',u'']
type_list_domestic_helper = ['果品市场','餐饮服务','糕饼店','冷饮店',]
type_list_office_building = ['中介机构', '公司', '公司企业','综合市场']
type_list_shopping_service = [u'购物服务','数码电子','综合市场','服装鞋帽皮具店','儿童用品店|购物服务', '品牌服装店', '专营店','室内设施']
type_list_tourism_service = ['住宿服务相关','交通地名', '地名地址信息']

type_list = [type_list_public_service,type_list_domestic_helper,type_list_office_building,type_list_shopping_service,type_list_tourism_service]
type_names = ['public-service','domestic-helper','office-building','shopping-convenience','tourism-convenience']
type_map = {
    'public-service':type_list_public_service,
    'domestic-helper':type_list_domestic_helper,
    'office-building':type_list_office_building,
    'shopping-convenience':type_list_shopping_service,
    'tourism-convenience':type_list_tourism_service
}
def map_type(poi):
    types = poi.get("types")
    scores = {}
    for name in type_names:
        scores[name] = 0.
    for type in types:
        i = 0
        for type_name,type_class in type_map.items():
            if type in type_class:
                scores[type_name] = 1.0

    return scores

def process_pois():
    print("start processing")
    collection = get_collection()
    print("get db")
    pois = collection.find()
    count = 0
    print("retrieve all data")
    for poi in pois:
        scores = map_type(poi)
        poi['scores'] = scores
        collection.update({'_id':poi['_id']},poi,True)
        count+=1
        if count % 5000==0:
            print(str(count)+' is done')
def modify_coordinates():
    collection = get_collection()
    pois = collection.find()
    i =0
    for poi in pois:
        if(i > 5):
            break
        coordinates = poi['loc']['coordinates']
        coordinates_converted = [coordinates[1],coordinates[0]]
        poi['loc']['coordinates'] = coordinates_converted
        collection.update({'_id':poi['_id']},poi,True)
        print(poi)

        i+=1
def prepare_process():
    type_texts = []
    collection = get_collection()
    pois = collection.find()
    pois_dimensions = []
    for poi in pois:
        poi_dimensions = []
        types = poi.get("types")
        for type in types:
            if type not in type_texts:
                type_texts.append(type)
            poi_dimensions.append(type_texts.index(type))
        pois_dimensions.append(poi_dimensions)
    dimension_size = len(type_texts)
    print(type_texts)
    data = []
    for poi_dimensions in pois_dimensions:
        poi_data = [0] * dimension_size
        for dimension_index in poi_dimensions:
            poi_data[dimension_index]+=1
        data.append(poi_data)
    data = np.array(data)
    print(data)
    return data,type_texts
def find_max_indx(my):
    pos = 0
    max = my[0]
    count = 0
    for i in my:
        if i > max:
            max = i
            pos =count
        count+=1
    my.remove(max)
    return pos
def parse_component(component):
    my = list(component)
    my_map = []
    for i in range(10):
        my_map.append(find_max_indx(my))
    return my_map
def pca(data):
    pca = PCA(n_components=14)
    pca.fit(data)
    return pca.components_;
    # my = list(pca.components_[3])
    # my_map = []
    # for i in range(len(my)):
    #     my_map.append(find_max_indx(my))
    #
    # return my_map
if __name__ == '__main__':
    # data,text = prepare_process()
    # tmp = pca(data)
    # k = list(map(parse_component,tmp))
    # for i in k:
    #     s = list(map(lambda x:text[x],i))
    #     print(s)
    # print(text[tmp])
    process_pois()
    # modify_coordinates()
