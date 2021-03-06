import os
from elasticsearch import Elasticsearch
import json
from datetime import datetime as dt
from datetime import timedelta,date


today_date  = date.today()
yesterday = today_date - timedelta(days = 1)

output_filename = f"{today_date}_output_eu.json"
diffrance_filename = f'{today_date}_Difference_Output_eu.json'
removed_filename = f'{today_date}_RemovedData_File_eu.json'


root = "/home/ubuntu/sanctions-scripts/Luxembourg/"

# root = ""
op_path = f"{root}OutputFiles"
dp_path = f"{root}DifferenceFiles"
rm_path = f"{root}RemovedFiles"

success_c = 0
failure_c = 0
failed_uids = []

rm_success_c = 0
rm_failed_c = 0

cid = "Test_Dev:YXAtc291dGgtMS5hd3MuZWxhc3RpYy1jbG91ZC5jb206OTI0MyQ2MGVhMDZlMjI1NTY0YzVkOGYzZmExOTkzMTljMTk5OCQ2MTdhYjA2MjMwZGY0MGI4ODAyZTRmMDE0ZTE1OGNkOA==" 
usrname="elastic"
pw = "3tkHE1qw9zpn5jFUkGfGRfjW" 

# db_name = "sanctionsdb"
db_name = "sanctionsnamelist1"

def get_data_from_file(fpath,fname):
    data = []
    file= f"{fpath}/{fname}"
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

def check_data_of_esdb(es,identity,article):
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

def remove_data_from_esdb(es,identity,article):
    global rm_success_c
    global rm_failed_c
    try:
        res = es.delete(index=db_name,id=identity)
        rm_success_c += 1
    except:
        print(f"Can not remove data of uid: {identity}")
        rm_failed_c += 1


if __name__ == "__main__":
    # es_db = Elasticsearch([{'host': '15.207.24.247', 'port': 9200}])
    # es_db = Elasticsearch("http://15.207.24.247:9272/", http_auth=('elasticsearch','kibana190166'))
    try:
        # es = Elasticsearch(cloud_id=cid,basic_auth=(usrname, pw))
        es = Elasticsearch([{'host': '15.207.24.247', 'port': 9200}])
        # file_data = get_data_from_file(dp_path,diffrance_filename)
        file_data = get_data_from_file(dp_path,diffrance_filename)
        if file_data != "NO FILE":
            print("Inserting Data from Diffrance File")
            for sdn in file_data:
                # print(file_data.index(sdn))
                write_data_to_esdb(es,sdn['uid'],sdn)
                # break
        else:
            file_data = get_data_from_file(dp_path,diffrance_filename)
            print("Checking Data From Output File")
            for sdn in file_data:
                # print(file_data.index(sdn))
                check_data_of_esdb(es,sdn['uid'],sdn)

        es.indices.refresh(index=db_name)
        print(f"Total Available : {len(file_data)}")
        print(f"Success : {success_c}")
        print(f"Failed  : {failure_c}")
        print("-----------------------------------------")
        print("Failed UIDS :")
        print(failed_uids)
        print("---*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*---")

        remo_data = get_data_from_file(rm_path,removed_filename)
        if remo_data and remo_data!="NO FILE":
            for rec in remo_data:
                remove_data_from_esdb(es,rec["uid"],rec)
    
        print(f"Total Records to be removed : {len(remo_data)}")
        print(f"Removed Succefully  : {rm_success_c}")
        print(f"Removed unuccefully : {rm_failed_c}")
        
    except Exception as e:
        print("-----------????ALERT????-----------")
        print("Uploading data to Elasticsearch Failed")
        print(f"Error : {e}")