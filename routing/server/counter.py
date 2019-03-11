import json
def count():
    file_path = './output/result0.json'
    with open(file_path, 'r') as fp:
        data = json.load(fp)
        print(len(data))
count()
