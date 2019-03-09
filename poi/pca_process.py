from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import BulkWriteError
import pandas as pd
import numpy as np
import os
from sklearn.decomposition import PCA
def get_collection():
    conn = MongoClient()
    db = conn.hangzhou
    return db.point_of_interest
a = []
type_list_public_service = [u'公共设施',u'急救中心']
type_list_domestic_helper = ['果品市场','餐饮服务','糕饼店','冷饮店','生活服务']
type_list_office_building = ['中介机构', '公司', '公司企业','综合市场']
type_list_shopping_service = [u'购物服务','数码电子','综合市场','服装鞋帽皮具店','儿童用品店|购物服务', '品牌服装店', '专营店','室内设施','购物相关场所']
type_list_tourism_service = ['住宿服务相关','交通地名', '地名地址信息']
type_list_transportation = ['火车站','交通地名']
type_list_vehicle_service = []
type_list = {
    'education':['培训机构|生活服务','学校','科教文化服务', '培训机构','科教文化场所'],
    'hospital':['医疗保健用品','医药保健销售店', '药房','医药保健相关','诊所', '医疗保健服务', '医疗保健服务场所','急救中心'],
    'sports':['综合体育馆','运动场所','公司|体育休闲服务','体育休闲服务场所','体育休闲服务', '运动场馆', '健身中心'],
    'public_transportation':[ '通行设施','交通设施服务', '公交车站', '公交车站相关', '道路附属设施'],
    'public_traffic':['改签','火车站', '站台'],
    'tourism':[ '风景名胜相关', '旅游景点','公园广场', '公园','公园景点售票处','风景名胜'],
    'vehicle_transportation':['高速路出口', '高速路入口',  '城市快速路出口','城市快速路入口','路边停车场','停车场出入口','公共停车场','警示信息', '违章停车','收费站','停车场', '停车场相关'],
    'accommodation':[   '旅馆招待所|住宿服务','旅馆招待所|住宿服务','三星级宾馆', '四星级宾馆','经济型连锁酒店', '青年旅舍', '五星级宾馆','宾馆酒店','旅馆招待所','住宿服务', '住宿服务相关'],
    'restaurant':['东北菜', '山东菜(鲁菜)','综合酒楼|餐饮服务', '潮州菜', '意式菜品餐厅', '意式菜品餐厅|体育休闲服务','意式菜品餐厅|餐饮服务','休闲餐饮场所|购物服务','休闲餐饮场所|餐饮服务', '安徽菜(徽菜)','茶餐厅','西餐厅(综合风味)|餐饮服务','四川菜(川菜)','湖南菜(湘菜)', '茶餐厅|餐饮服务', '广东菜(粤菜)','韩国料理','休闲餐饮场所',  '宾馆酒店','日本料理','休闲餐饮场所', '宾馆酒店','海鲜酒楼', '火锅店','外国餐厅', '西餐厅(综合风味)','餐饮服务', '中餐厅', '浙江菜', '特色/地方风味餐厅', '餐饮相关场所', '餐饮相关'],
    'leisure':['休闲场所'],
    'cafe':['甜品店|购物服务','酒吧|餐饮服务','快餐厅', '咖啡厅','甜品店'],
    'cake':['糕饼店|餐饮服务','糕饼店','冷饮店','冷饮店|餐饮服务'],
    'vehicle':[  '汽车俱乐部','福特特约销售', '福特销售', '服装鞋帽皮具店|汽车服务', '汽车租赁','洗车场','充电站','汽车综合维修','汽车维修','汽车养护/装饰', '汽车养护','汽车配件销售|汽车销售', '汽车销售','汽车服务相关','汽车服务', '汽车配件销售'],
    'convenience':['专营店|生活服务', '生活服务场所','便民商店/便利店|购物服务','生活服务','便民商店/便利店'],
    'photography':['摄影冲印店', '摄影冲印'],
    'beauty':[ '美容美发店|餐饮服务','美容美发店','美容美发店|购物服务'],
    'government':[ '公检法机构', '公检法机关', '政府及社会团体相关','政府机构及社会团体', '交通车辆管理', '交通管理机构'],
    'society':['社会团体', '社会团体相关','社会团体', '社会团体相关'],
    'mall':['普通商场','购物中心', '商场','室内设施', '购物服务', '超级市场', '超市'],
    'book_store':['书店'],
    'market':['厨卫市场|生活服务', '灯具瓷器市场' '布艺市场|购物服务','布艺市场','厨卫市场','小商品市场','果品市场|餐饮服务', '农副产品市场','建材五金市场', '蔬菜市场','家居建材市场', '家具城','综合市场', '果品市场','花鸟鱼虫市场', '花卉市场'],
    'shops':[ '生活服务场所|购物服务','建筑物门|购物服务', '购物服务', '购物相关场所','专卖店', '专营店','专营店|购物服务'],
    'bike_shop':['摩托车服务', '摩托车维修','自行车专卖店','自行车专卖店|生活服务','维修站点'],
    'travel_agency':['旅行社'],
    'digital_shop':['手机销售|购物服务', '综合家电商场','手机销售','家电电子卖场', '数码电子'],
    'stationery':['礼品饰品店|购物服务','文化用品店', '礼品饰品店',],
    'entertainment':[ 'KTV|体育休闲服务', '洗浴推拿场所|体育休闲服务','溜冰场','游乐场','影剧院相关','游戏厅','牛扒店(扒房)', 'KTV','影剧院', '电影院','棋牌室', '台球厅','洗浴推拿场所','娱乐场所', '网吧','酒吧', '茶艺馆'],
    'business_service':[ '电讯营业厅', '中国电信营业厅','公证鉴定机构','银行相关','中介机构', '金融保险服务','金融保险服务机构', '金融保险机构', '自动提款机', '二手车交易'],
    'glasses_store':['眼镜店'],
    'cigarette_store':['烟酒专卖店','音像店'],
    'domestic_helper':['洗衣店',],
    'luxury':['古玩字画店','钟表店','钟表店|购物服务'],
    'sports_store':['户外用品|购物服务','体育用品店|购物服务', '体育用品店', '户外用品'],
    'pet_store':[ '宠物用品店','宠物市场', '宠物用品店|购物服务','宠物用品店|科教文化服务',],
    'children_store':['儿童用品店','儿童用品店|购物服务'],
    'boutique':['品牌鞋店|购物服务','品牌皮具店', '品牌箱包店', '品牌服装店|购物服务', '服装鞋帽皮具店|购物服务','阿迪达斯专卖店','服装鞋帽皮具店|生活服务','珠宝首饰工艺品','服装鞋帽皮具店','品牌服装店','品牌鞋店','个人用品/化妆品店', '其它个人用品店'],
    'business':[ '建筑公司', '事务所', '会计师事务所', '公司|生活服务','公司企业', '公司','网络科技','医药公司','楼宇','商务写字楼'],
    'advertisement_store':[ '广告装饰'],
    'media':['传媒机构','杂志社', '报社'],
    'research':['科研机构'],
    'roads':['桥','交通地名', '道路名','路口名'],
    'address':['建筑物门', '临街院门', '临街院正门','地名地址信息','门牌信息', '楼栋号' ],
    'house':[ '住宅小区','住宅区', '宿舍','楼栋号','商务住宅', '商务住宅相关'],
    'event':['节日庆典','会展中心', '事件活动', '公众活动', '展会展览'],
    'logistics':[ '物流速递'],
    'agency':[ '售票处','火车票代售点','售票|生活服务'],
    'public_facility':[ '公共设施', '公共厕所'],
    'factory':['工厂','产业园区'],
    'skill_education':['驾校',],
    'museum':[ '展览馆','美术馆', '综合家电商场|购物服务'],
    'equipment_store':['家电电子卖场|购物服务',],
    'airport':[ '机场相关', '机场出发/到达']


}

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
        if count == 5000:
            print(str(count)+' is done')
def modify_coordinates():
    collection = get_collection()
    pois = collection.find()
    i =0
    for poi in pois:
        # if(i > 5):
        #     break
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
    return pca.components_
    # my = list(pca.components_[3])
    # my_map = []
    # for i in range(len(my)):
    #     my_map.append(find_max_indx(my))
    #
    # return my_map
def get_category():
    dir_path = '../data/poi_type/'
    file_names = os.listdir(dir_path)
    my_category = {}
    for file_name in file_names:
        with open(dir_path+file_name,'r',encoding='utf-8') as file:
            print(list(map(lambda x:x.strip(),file.readlines())))
            my_category[file_name[:-4]] = list(map(lambda x:x.strip(),file.readlines()))
            print(file_name[:-4])
    return my_category

if __name__ == '__main__':
    category = get_category()

    # data,text = prepare_process()
    # tmp = pca(data)
    # k = list(map(parse_component,tmp))
    # for i in k:
    #     s = list(map(lambda x:text[x],i))
    #     print(s)
    # print(text[tmp])
    # process_pois()
    # modify_coordinates()