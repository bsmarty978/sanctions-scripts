import json 
import hashlib
from elasticsearch import Elasticsearch

            # "covers" : "1",
data = [
    {
        "last_updated": "2022-05-19T09:56:13",
        "sanction_list": {
        "sl_authority": "Mexican Insurance and Surety National Commission (Comisión Nacional de Seguros y Fianzas de México), Mexico",
        "sl_url": "https://www.gob.mx/cnsf/documentos/sanciones-a-instituciones-de-seguros",
        "sl_host_country": "Mexico",
        "covers" : "1",
        "sl_type": "Sanctions",
        "watch_list": "South and Central America Watchlists",
        "sl_source": "Mexican Insurance and Surety National Commission (Comisión Nacional de Seguros y Fianzas de México) Sanctions List, Mexico",
        "sl_description": "List of defaulter Directors by Ministry of Corporate Affairs, India.",
        "list_id": "MEX_E20209"
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