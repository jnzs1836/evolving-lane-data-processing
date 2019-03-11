data_file = r'tb_bike_order_1804.csv'
output_file_size = 5000
record_file = './routing/record.txt'
import requests
import pandas as pd
import json
from routing.server.coordinates import gcj02towgs84
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
def process_od():
    file_count = 0
    csv = pd.read_csv(data_file)
    print(csv.shape)
    count = 0
    store = []
    for i in range(2725450):
        if file_count < tmp + 1:
            count += 1
            if count == output_file_size:
                file_count += 1
                count = 0
            continue
        prefix = 'http://localhost:8989/route?'
        point = 'point='
        split = '%2C'
        origin = gcj02towgs84(float(csv.iloc[i,5]),float(csv.iloc[i,6]))
        destination = gcj02towgs84(float(csv.iloc[i,8]),float(csv.iloc[i,9]))
        tail = '&vehicle=bike&details=street_name&details=edge_id&points_encoded=false&details=time'
        url = prefix + point + str(origin[1]) + split + str(origin[0]) + '&' + point + str(destination[1]) + split + str(destination[0]) + tail
        # print(csv.iloc[1, 5])
        r = requests.get(url)
        count+=1
        parsed_data = json.loads(r.text)
        doc = {
            'Id': i,
            'MsgId':csv.iloc[i,0],
            'CompanyId':csv.iloc[i,1],
            'BicycleNo':csv.iloc[i,2],
            'OrderId':csv.iloc[i,3],
            'DepartTime':csv.iloc[i,4],
            'DepartLongitude':csv.iloc[i,5],
            'DepartLatitude':csv.iloc[i,6],
            'ArriveTime':csv.iloc[i,7],
            'ArriveLongitude':csv.iloc[i,8],
            'ArriveLatitude':csv.iloc[i,9],
            'DBTime':csv.iloc[i,10],
            'Distance':csv.iloc[i,11],
            'Route':parsed_data,
        }
        store.append(doc)
        if count == output_file_size:
            with open('./output2/result'+ str(file_count)+'.json', 'w') as fp:
                json.dump(store, fp)
                store = []
                file_count+=1
            count = 0
            with open(record_file,'w+') as fp:
                fp.write(str(file_count-1) +'is done')


if __name__ == '__main__':
    process_od()
    # get_points()
