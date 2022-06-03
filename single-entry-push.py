import json 
import hashlib
from elasticsearch import Elasticsearch

            # "covers" : "1",
data = [
    {
        "last_updated": "2022-06-03T12:23:05",
        "sanction_list": {
            "sl_authority": "Metropolitan Stock Exchange, India",
            "sl_url": "https://www.msei.in/Investors/defaulters",
            "watch_list": "India Watchlists",
            "sl_host_country": "India",
            "covers" : "1",
            "sl_type": "Sanctions",
            "sl_source": "MSE Expelled Members, India",
            "sl_description": "list of Debarred Entities by Metropolitan Stock Exchange, India",
            "list_id": "IND_E20322"
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
        # res = es.index(index=dbName, id=record['uid'], body=record, op_type = 'create')
        res = es.index(index=dbName, id=record['uid'], body=record) # NOTE if It need to be updated
        print(res)
        i +=1
    except Exception as ex:
        print("already created",ex)
        i +=1    