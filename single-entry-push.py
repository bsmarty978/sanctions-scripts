import json 
import hashlib
from elasticsearch import Elasticsearch

            # "covers" : "1",
data = [
    {
        "last_updated": "2022-06-01T07:13:12",
        "sanction_list": {
            "sl_authority": "MCX Regulatory Defaulting Clients, India",
            "sl_url": "https://www.bseindia.com/",
            "sl_host_country": "India",
            "sl_type": "Sanctions",
            "covers" : "1",
            "sl_source": "MCX Regulatory Defaulting Clients, India",
            "sl_description": "MCX Regulatory Defaulting Clients, India",
            "watch_list": "India Watchlists",
            "list_id": "IND_E20310"
        }
        }
    ]
        

lst = []
for entry in data:
    result = hashlib.md5((entry['sanction_list']['sl_source']).encode())
    entry['uid'] = result.hexdigest()
    lst.append(entry)

es = Elasticsearch([{'host': '15.207.24.247', 'port': 9200}])
dbName = 'sanctionsources'


i = 0
for record in lst:
    print(i)
    try:
        res = es.index(index=dbName, id=record['uid'], body=record, op_type = 'create')
        # res = es.index(index=dbName, id=record['uid'], body=record) # NOTE if It need to be updated
        print(res)
        i +=1
    except Exception as ex:
        print("already created",ex)
        i +=1    