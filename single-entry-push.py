import json 
import hashlib
from elasticsearch import Elasticsearch

            # "covers" : "1",
data = [
    {
        "last_updated": "2022-06-15T12:23:05",
        "sanction_list": {
            "sl_authority": "The Securities Commission Malaysia (SC), Malaysia",
            "sl_url": "https://www.sc.com.my/regulation/enforcement/investor-alerts/sc-investor-alerts/investor-alert-list",
            "watch_list": "APAC Watchlists",
            "sl_host_country": "Malaysia",
            "covers" : "1",
            "sl_type": "Sanctions",
            "sl_source": "The Securities Commission Malaysia Investor Alert List, Malaysia",
            "sl_description": "List of unauthorised websites, investment products, companies and individuals issued by The Securities Commission Malaysia (SC), Malaysia",
            "list_id": "MYS_E20199"
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