data_file = r'tb_bike_order_1804.csv'
output_file_size = 50000
record_file = './routing/record.txt'
import requests
import pandas as pd
import json
import time
from routing.server.coordinates import gcj02towgs84

from multiprocessing import Pool

def get_points():
    file_count = 0
    csv = pd.read_csv(data_file)
    print(csv.shape)
    count = 0
    points = []
    for i in range(2720):
        points.append([csv.iloc[i,6],csv.iloc[i,5]])
    print(points)
tmp =0
def print_shape():
    file_count = 0
    print("here")
    csv = pd.read_csv(data_file)
    print('read')
    print(csv.shape)
prefix = 'http://localhost:8989/route?'
point = 'point='
split = '%2C'
def route_od(item):
    # item = one[4-3]

    # id = one[4-4]
    origin = gcj02towgs84(float(item.iloc[ 5]), float(item.iloc[6]))
    destination = gcj02towgs84(float(item.iloc[ 8]), float(item.iloc[ 9]))
    tail = '&vehicle=bike&details=street_name&details=edge_id&points_encoded=false&details=time'
    url = prefix + point + str(origin[1]) + split + str(origin[0]) + '&' + point + str(destination[1]) + split + str(
        destination[0]) + tail
    # print(csv.iloc[1, 5])
    r = requests.get(url)

    parsed_data = json.loads(r.text)
    doc = {
        # 'Id': id
        'MsgId': str(item.iloc[0]),
        'CompanyId': item.iloc[ 1],
        'BicycleNo': str(item.iloc[ 2]),
        'OrderId': str(item.iloc[ 3]),
        'DepartTime': item.iloc[ 4],
        'DepartLongitude': item.iloc[ 5],
        'DepartLatitude': item.iloc[ 6],
        'ArriveTime': item.iloc[ 7],
        'ArriveLongitude': item.iloc[ 8],
        'ArriveLatitude': item.iloc[ 9],
        'DBTime': item.iloc[10],
        'Distance': item.iloc[ 11],
        'Route': parsed_data,
    }
    return doc

def process_od():
    file_count = 0
    csv = pd.read_csv(data_file)
    print(csv.shape)
    # csv = csv.iloc[0:5000,:]
    count = 0
    store = []
    tmp = -1

    with open(record_file,'r') as fp:
        tmp = int(fp.readlines()[-1])
        print(tmp)
    data = []

    for i in range(13352813):
        data.append(csv.iloc[i,:])
        if i % output_file_size ==0 and i!=0:
            start = time.time()
            with Pool(150) as p:
                store = p.map(route_od,data)
                stop = time.time()
                print('file' + str(file_count) + ': '+ str(stop-start))
                data = []
                with open('./data/trajectory/01routing/' + str(file_count) + '.json', 'w') as fp:
                    # print(store)
                    json.dump(store, fp)
                    file_count+=1
                with open(record_file,'a+') as fp:
                    print(file_count-1)
                    fp.write(str(file_count-1))

    # print(store)
    # for i in range(13352813):
        # if file_count < tmp + 1:
        #     count += 1
        #     if count == output_file_size:
        #         file_count += 1
        #         count = 0
        #     continue
        #
        # prefix = 'http://localhost:8989/route?'
        # point = 'point='
        # split = '%2C'
        # origin = gcj02towgs84(float(csv.iloc[i,5]),float(csv.iloc[i,6]))
        # destination = gcj02towgs84(float(csv.iloc[i,8]),float(csv.iloc[i,9]))
        # tail = '&vehicle=bike&details=street_name&details=edge_id&points_encoded=false&details=time'
        # url = prefix + point + str(origin[1]) + split + str(origin[0]) + '&' + point + str(destination[1]) + split + str(destination[0]) + tail
        # # print(csv.iloc[1, 5])
        # r = requests.get(url)
        # count+=1
        # parsed_data = json.loads(r.text)
        # doc = {
        #     'Id': i,
        #     'MsgId':csv.iloc[i,0],
        #     'CompanyId':csv.iloc[i,1],
        #     'BicycleNo':csv.iloc[i,2],
        #     'OrderId':csv.iloc[i,3],
        #     'DepartTime':csv.iloc[i,4],
        #     'DepartLongitude':csv.iloc[i,5],
        #     'DepartLatitude':csv.iloc[i,6],
        #     'ArriveTime':csv.iloc[i,7],
        #     'ArriveLongitude':csv.iloc[i,8],
        #     'ArriveLatitude':csv.iloc[i,9],
        #     'DBTime':csv.iloc[i,10],
        #     'Distance':csv.iloc[i,11],
        #     'Route':parsed_data,
        # }
        # store.append(doc)
        # print(count)
        # if count %300 ==0:
        #     print(count)
        # if count == output_file_size:
        #     with open('./data/trajectory/01routing/'+ str(file_count)+'.json', 'w') as fp:
        #         json.dump(store, fp)
        #         store = []
        #         file_count+=1
        #     count = 0
        #     with open(record_file,'a+') as fp:
        #         print(file_count-1)
        #         fp.write(str(file_count-1))


if __name__ == '__main__':
    process_od()
    # get_points()
