import requests
r = requests.get('http://localhost:8989/route?point=30.312176%2C120.38728&point=30.32299%2C120.34988')
print(r.text)
import json
sample = [r.text]
with open('result.json', 'w') as fp:
    json.dump(sample, fp)

