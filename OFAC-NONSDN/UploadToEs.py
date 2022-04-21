import os
from elasticsearch import Elasticsearch
import json
from datetime import datetime
import time


#diffrance_filename = f'diffrance-nonsdn-json-{today_date.day}-{today_date.month}-{today_date.year}.json'

def get_data_from_file():
    data = []
    lst=['diffrance-nonsdn-json-17-2-2022.json']
    for file in lst:
        # file = 'pushed\\'+ file
        with open(file,'rb') as f:
            data += json.load(f)
        print(len(data))
    return data

def write_data_to_esdb(es,identity,article):
    try:
        res = es.index(index="sanctionsnamelist1", id=identity, body=article, op_type = 'create')
        print(res)
    except Exception as ex:
        print("already created",ex)
    

if __name__ == "__main__":
    es_db = Elasticsearch([{'host': '15.207.24.247', 'port': 9200}])
    # es_db = Elasticsearch("http://15.207.24.247:9272/", http_auth=('elasticsearch','kibana190166'))

    file_data = get_data_from_file()
    cnt = 0
    for sdn in file_data[:]:
        print(file_data.index(sdn))
        write_data_to_esdb(es_db,sdn['uid'],sdn)
        break
