import json 
import hashlib
from elasticsearch import Elasticsearch

            # "covers" : "1",
data = [
    {
        "last_updated": "2022-05-26T09:56:13",
        "sanction_list": {
            "sl_authority": "Japan Public Security Intelligence Agency Terrorists Ogranisations, Japan",
            "sl_url": "https://www.moj.go.jp/",
            "sl_host_country": "Japan",
            "covers" : "1",
            "sl_type": "Sanctions",
            "sl_source": "Japan Public Security Intelligence Agency Terrorists Ogranisations, Japan",
            "sl_description": "Japan Public Security Intelligence Agency Terrorists Ogranisations, Japan",
            "watch_list": "APAC Watchlists",
            "list_id": "JPN_T30104"
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