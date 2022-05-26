import os
from elasticsearch import Elasticsearch
import json
from datetime import datetime as dt
from datetime import timedelta,date

success_c = 0
failure_c = 0
failed_uids = []

rm_success_c = 0
rm_failed_c = 0


db_name = "sanctionsnamelist1"
# db_name = "pepnamelist-01"


def get_data_from_file(fname):
    data = []
    file= f"{fname}"
    try:
        with open(file,'rb') as f:
            data += json.load(f)
        print(f"Available Data: {len(data)}")
        return data
    except FileNotFoundError:
        return "NO FILE"

def write_data_to_esdb(es,identity,article):
    global success_c
    global failure_c
    global failed_uids
    try:
        res = es.index(index=db_name,id=identity, body=article)
        success_c += 1
    except Exception as ex:
        failure_c += 1
        print(f"Data Inserting Error For UID : {identity}")
        failed_uids.append(identity)
        print(ex)
        # res = es.update(index="secnc", document=article)


if __name__ == "__main__":
    try:
        es = Elasticsearch(['15.207.24.247:9200'])
        file_data = get_data_from_file("/home/ubuntu/sanctions-scripts/JP-PSIA/OutputFiles/2022-05-26_output_sng-771.ch.json")
        if file_data != "NO FILE":
            print("Inserting Data from Given File")
            for sdn in file_data:
                # print(file_data.index(sdn))
                write_data_to_esdb(es,sdn['uid'],sdn)
                # break

                
        es.indices.refresh(index=db_name)
        print(f"Total Available to Push: {len(file_data)}")
        print(f"Success : {success_c}")
        print(f"Failed  : {failure_c}")
        print("--*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*")
        print("Failed UIDS :")
        # print(failed_uids)
        print("---*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*---")

    except Exception as e:
        print("-----------🛑ALERT🛑-----------")
        print("Uploading data to Elasticsearch Failed")
        print(f"Error : {e}")
