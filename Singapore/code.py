import json
import hashlib
import datetime
from datetime import datetime,date,timedelta
import requests
from copy import deepcopy
import os
from os.path import exists
import boto3



#NOTE: Object for output json file
out_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "Singapore"

# input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.xlsx'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/Singapore/"
# root = ""
# ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def process_data():
    global out_list,last_updated_string,total_profile_available

    url = "https://www.mas.gov.sg/api/v1/ialsearch?json.nl=map&wt=json&sort=date_dt%20desc&q=*:*&rows=1&start=0"
    headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36 Edg/102.0.1245.33","Content-Type": "application/json, text/plain, */*","Host" : "www.mas.gov.sg"}
    resp = requests.get(url, headers= headers)
    # print(resp)
    data = json.loads(resp.text)
    num = data['response']['numFound']

    new_url = f"https://www.mas.gov.sg/api/v1/ialsearch?json.nl=map&wt=json&sort=date_dt%20desc&q=*:*&rows={num}&start=0"
    resp = requests.get(new_url, headers= headers)
    data = json.loads(resp.text)

    for i in data['response']['docs']:
        d = {
            "uid": "",
            "name": "",
            "alias_name": [],
            "country": [],
            "list_type": "Entity",
            "last_updated": last_updated_string,
            "list_id": "SGP_E20249",
            "nns_status": "False",
            "address": [
            {
            "country": "",
            "complete_address": ""
            }
            ],
            "documents": {},
            "comment": "",
            "sanction_details":{},
            "entity_details":{
                "email":[],
                "website":[]
            },
            "sanction_list": {
                "sl_authority": "Monetary Authority of Singapore, Singapore",
                "sl_url": "https://www.mas.gov.sg/investor-alert-list",
                "watch_list": "APAC Watchlists",
                "sl_host_country": "Singapore",
                "sl_type": "Sanctions",
                "sl_source": "Monetary Authority of Singapore investor Alert List, Singapore",
                "sl_description": "list of unregulated entites who have been wrongly perceived as being licensed or regulated by Monetary Authority of Singapore, Singapore"
            }
        }
        name = i['unregulatedpersons_t'][0]
        if name!="":
            d["name"]=name
        
        ali =  i['unregulatedpersons_t'][1:]
        for k in ali:
            if "/" in k:
                new_ali = k.split(" / ")
                d["alias_name"] = d["alias_name"] + new_ali
            else:
                d["alias_name"].append(k) 

        if "/" in name:
            alis = name.split(" / ")
            d["alias_name"] = d["alias_name"] + alis[1:]
            d["name"] = alis[0]

        for j in i['alternativename_t']:
            if j.strip()!="":
                d["alias_name"].append(j)


        d["uid"] = hashlib.sha256(
            ((d["name"] + i['id'] + d["sanction_list"]["sl_source"]+d["sanction_list"]["sl_host_country"]).lower()).encode()).hexdigest()

        remail = i['email_s'].split("\n")
        email = [k for k in remail if k.strip()]
        d["entity_details"]["email"] = email

        rweb = i['website_s'].split("\n")
        web = [k for k in rweb if k.strip()]
        d["entity_details"]["website"] = web


        address  = i['address_s']
        if address!="":
            d["address"][0]["complete_address"] = address

        if d["name"]!="":
            out_list.append(d)

    total_profile_available = len(out_list)
    print(f"Total profile available: {total_profile_available}")
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=4)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=4)

def CompareDocument():
    try:
        with open(f'{op_path}/{old_output_filename}', 'rb') as f:
            data = f.read()
    except:
        print("---------------------Alert--------------------------")
        print(f"There is not any old file for date: {yesterday.ctime()}")
        print("----------------------------------------------------")
        data = "No DATA"
    old_list = []
    if data != "No DATA":   
        old_list = json.loads(data)

    with open(f'{op_path}/{output_filename}', 'rb') as f:
        new_data = f.read()
    new_list =  json.loads(new_data)

    new_profiles = []
    removed_profiles = []
    updated_profiles = []

    old_dict = {}
    if old_list:
        for val in old_list:
            old_dict[val["uid"]] = val
        
    new_uid_list = []
    for val1 in new_list:
        new_uid = val1["uid"]
        new_uid_list.append(new_uid)
        if new_uid in old_dict.keys():
            # print("Already in List")
            for i in val1:
                if i!= "last_updated":
                    try:
                        if val1[i] != old_dict[val1["uid"]][i]:
                            # print(f"Updataion Detected on: {val1['uid']} for: {i}")
                            # print(f"Updataion Detected for: {val1[i]}")
                            updated_profiles.append(val1)
                            break
                    except:
                        # print(f"Updataion Detected on: {val1['uid']} for: {i}")
                        # print(f"Updataion Detected for: {val1[i]}")
                        updated_profiles.append(val1)
                        break

        else:
            new_profiles.append(val1)
            # print(f"New Profile Detected : {val1['uid']}")
    

    for val2 in old_dict.keys():
        if val2 not in new_uid_list:
            # print(f"Removed Profile Detected : {val2}")
            removed_profiles.append(old_dict[val2])

    print("------------------------LOG-DATA---------------------------")
    print(f"total New Profiles Detected:     {len(new_profiles)}")
    print(f"total Updated Profiles Detected: {len(updated_profiles)}")
    print(f"total Removed Profiles Detected: {len(removed_profiles)}")
    print("-----------------------------------------------------------")

    if len(new_list)==0:
        removed_profiles = []
        raise ValueError("Error : Data Parsing Error.... fix it quick ⚒️")
    try:
        with open(f'{dp_path}/{diffrance_filename}', "w",encoding='utf-8') as outfile:
            json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)
    except FileNotFoundError:
        os.mkdir(dp_path)
        with open(f'{dp_path}/{diffrance_filename}', "w",encoding='utf-8') as outfile:
            json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)

    try:
        with open(f'{rm_path}/{removed_filename}', "w",encoding='utf-8') as outfile:
            json.dump(removed_profiles, outfile,ensure_ascii=False)
    except FileNotFoundError:
        os.mkdir(rm_path)
        with open(f'{rm_path}/{removed_filename}', "w",encoding='utf-8') as outfile:
            json.dump(removed_profiles, outfile,ensure_ascii=False)

    if exists(lp_path):
        with open(f'{lp_path}',"a") as outfile:
            passing = f"{last_updated_string},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(passing)
    else:
        with open(f'{lp_path}',"a") as outfile:
            pass_first = "date,outputfile,total_profile_availabe,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
            passing = f"{last_updated_string},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(pass_first)
            outfile.write(passing)

def UploadfilestTos3():
    try:
        print("uploading files to s3")
        s3 = boto3.client('s3')
        # s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"{dag_name}/original/{input_filename}")
        s3.upload_file(f'{op_path}/{output_filename}',"sams-scrapping-data",f"{dag_name}/parced/{output_filename}")
        s3.upload_file(f'{dp_path}/{diffrance_filename}',"sams-scrapping-data",f"{dag_name}/diffrance/{diffrance_filename}")
        s3.upload_file(f'{rm_path}/{removed_filename}',"sams-scrapping-data",f"{dag_name}/removed/{removed_filename}")
        s3.upload_file(f'{lp_path}',"sams-scrapping-data",f"{dag_name}/{lp_name}")
        print("uploaded files to s3")      
    except Exception as e:
        print("------------------🔴ALERT🔴------------------------")
        print("Can not upload files to s3")
        print("Exception : " , e)
        print("----------------------------------------------------")


process_data()
# CompareDocument()
# UploadfilestTos3()