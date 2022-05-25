import json 
import hashlib
from elasticsearch import Elasticsearch

            # "covers" : "1",
data = [
    {
        "last_updated": "2022-05-25T09:56:13",
        "sanction_list": {
            "sl_authority": "Financial Monitoring Service of the Republic Of Azerbaijan, Azerbaijan",
            "sl_url": "http://www.fiu.az/en/sanctions/internal-sanctioned",
            "watch_list": "EMEA Watchlists",
            "covers" : "1",
            "sl_type": "Sanctions",
            "sl_host_country": "Azerbaijan",
            "sl_source": "Financial Monitoring Service Sanctions Notices List, Azerbaijan",
            "sl_description": "List of individuals who are sanctioned by Financial Monitoring Service of the Republic Of Azerbaijan, Azerbaijan",
            "list_id": "AZE_S10014"
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