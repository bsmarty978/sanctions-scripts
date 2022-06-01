import json 
import hashlib
from elasticsearch import Elasticsearch

            # "covers" : "1",
data = [
    {
        "last_updated": "2022-03-03T12:23:05",
          "sanction_list" : {
            "sl_authority" : "INTERPOL",
            "sl_url" : "https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals",
            "sl_host_country" : "United States of America",
            "sl_type" : "Sanctions",
            "list_id" : "INT_T30093",
            "sl_source" : "Interpol-United Nations Security Council Special Notices for Individuals",
            "sl_description" : "The INTERPOL-United Nations Security Council Special Notice alerts global police to individuals and entities that are subject to sanctions imposed by the United Nations Security Council. The three most common sanctions are assets freeze, travel ban and arms embargo.",
            "watch_list" : "Global Watchlists"
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