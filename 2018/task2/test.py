from time import sleep

import requests

endpoint = 'http://127.0.0.1:8080/api/md5'
url = 'https://upload.wikimedia.org/wikipedia/commons/4/49/Allied_Carpets_-_Crown_Point_Retail_Park_-_geograph.org.uk_-_1145711.jpg'
wrong_url = '--'

resp = requests.post(endpoint, json={'url': url})
assert resp.status_code == 200
resp_json = resp.json()
assert resp_json
task_id = resp_json['id']

resp = requests.get(endpoint, params={'task_id': task_id})
assert resp.status_code == 200
resp_json = resp.json()
assert resp_json
assert resp_json['status'] == 'not ready'
assert not resp_json['result']

sleep(2)
resp = requests.get(endpoint, params={'task_id': task_id})
assert resp.status_code == 200
resp_json = resp.json()
assert resp_json
assert resp_json['status'] == 'ready'
assert resp_json['result']

resp = requests.post(endpoint, json={'url': wrong_url})
assert resp.status_code == 200
resp_json = resp.json()
assert resp_json
task_id = resp_json['id']

resp = requests.get(endpoint, params={'task_id': task_id})
assert resp.status_code == 200
resp_json = resp.json()
assert resp_json
assert resp_json['status'] == 'not ready'
assert not resp_json['result']

sleep(2)
resp = requests.get(endpoint, params={'task_id': task_id})
assert resp.status_code == 200
resp_json = resp.json()
assert resp_json
assert resp_json['status'] == 'has error'
assert resp_json['result']
