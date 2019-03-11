import json
import os
file_path = './output/result0.json'
file_path_for_js = './converted/result0.txt'
dir_path= './output2/'
out_dir_path = './converted2/'
def read_file_routes(file,count):
    with open(dir_path + file, 'r') as fp:
        with open(out_dir_path + 'result' + str(count)+'.txt', 'w') as out:
            data = json.load(fp)
            for trajectory in data:
                parsed_data  = json.loads(trajectory)
                try:
                    out.write(parsed_data['paths'][0]['points'] + '\n')
                except Exception:
                    print(parsed_data)
def read_routes():
    count = 0
    file_list = os.listdir('./output2')
    for file in file_list:
        read_file_routes(file,count)
        count+=1
        print('file'+str(count)+' is converted')


if __name__ == '__main__':
    read_routes()

