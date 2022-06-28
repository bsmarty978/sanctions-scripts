import json 
import hashlib
from elasticsearch import Elasticsearch

            # "covers" : "1",
data = [
    {
        "last_updated": "2022-06-28T12:23:05",
        "sanction_list": {
        "sl_authority": "Secretariat of the Civil Service (Secretaría de la Función Pública), Mexico",
        "sl_url": "https://directoriosancionados.funcionpublica.gob.mx/SanFicTec/jsp/Ficha_Tecnica/SancionadosN.htm",
        "sl_host_country": "Mexico",
        "covers" : "1",
        "list_id" : "MEX_S10052",
        "sl_type": "Sanctions",
        "watch_list": "South and Central America Watchlists",
        "sl_source": "Secretariat of the Civil Service (Secretaría de la Función Pública) Sanctions List, Mexico",
        "sl_description": "Sanctions List issued by Secretariat of the Civil Service (Secretaría de la Función Pública), Mexico."
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