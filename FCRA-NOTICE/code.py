"FCRA-NOTICE"

import camelot
import json
import hashlib
import datetime
from datetime import datetime,date,timedelta
import os
from os.path import exists
import boto3
from copy import deepcopy
import requests


#NOTE: Object for output json file
out_list = []
input_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "fcra-notice"

input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.pdf'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/FCRA-NOTICE/"
# root = ""
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def pdfdownloder(url):
    response = requests.get(url)
    try:
        with open(f'{ip_path}/{input_filename}', 'wb') as f:
            f.write(response.content)
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename}', 'wb') as f:
            f.write(response.content)


def fileprocessor():
    tb = camelot.read_pdf(f"{ip_path}/{input_filename}",pages="all",flavor="stream")
    global outlist,total_profile_available
    c = 0
    for i in tb:
        tbdf = i.df
        if tbdf.columns.stop > 1:
            for j in tbdf.itertuples():
                if j[-2].split(" ")[-1] not in ["Registration","Number","Fcra Registration","Registration\nNumber","No.\nNumber",""]:
                    rid = j[-2].split(" ")[-1]
                    out_list.append(
                        {
                            "uid" : hashlib.sha256(((rid+j[-1]+"Indian FCRA 05").lower()).encode()).hexdigest(),
                            "name" : j[-1],
                            "alias_name":[],
                            "country": ["India"],
                            "list_type": "Entity",
                            "last_updated": last_updated_string,
                            "entity_details": {},
                            "list_id": "IND_E20004",
                            "nns_status": "False",
                            "address": [
                                {
                                    "complete_address":"",
                                    "country":"India"
                                }
                            ],
                            "sanction_Details":{},
                            "documents":{"Registration":rid},
                            "comment" : "",
                            "sanction_list" :{
                                "sl_authority": "Ministry of Home Affairs, India",
                                "sl_url": "",
                                "sl_host_country": "India",
                                "sl_type": "Sanctions",
                                "watch_list": "India Watchlists",
                                "sl_source": "The Foreign Contribution (Regulation) Act, Public notices List, India",
                                "sl_description": "List of Public notices List by The Foreign Contribution (Regulation) Act of Ministry of Home Affairs, India."
                                }
                        }
                    )
        else:
            print(f"Boom : {c}")
        c+=1

    total_profile_available = len(out_list)
    print(f"Total Available Profiles : {total_profile_available}")
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile,ensure_ascii=False,indent=2)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
                json.dump(out_list, outfile,ensure_ascii=False,indent=2)     

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
    new_list = deepcopy(out_list)

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
                            print(f"Updataion Detected on: {val1['uid']} for: {i}")
                            # print(f"Updataion Detected for: {val1[i]}")
                            updated_profiles.append(val1)
                            break
                    except:
                        print(f"Updataion Detected on: {val1['uid']} for: {i}")
                        # print(f"Updataion Detected for: {val1[i]}")
                        updated_profiles.append(val1)
                        break

        else:
            new_profiles.append(val1)
            print(f"New Profile Detected : {val1['uid']}")
    

    for val2 in old_dict.keys():
        if val2 not in new_uid_list:
            print(f"Removed Profile Detected : {val2}")
            removed_profiles.append(old_dict[val2])
    if len(new_list)==0:
        removed_profiles = []
        raise ValueError("Error : Data Parsing Error.... fix it quick ⚒️")

    print("------------------------LOG-DATA---------------------------")
    print(f"total New Profiles Detected:     {len(new_profiles)}")
    print(f"total Updated Profiles Detected: {len(updated_profiles)}")
    print(f"total Removed Profiles Detected: {len(removed_profiles)}")
    print("-----------------------------------------------------------")

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
            passing = f"{last_updated_string},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(passing)
    else:
        with open(f'{lp_path}',"a") as outfile:
            pass_first = "date,inputfile,outputfile,total_profile_availabe,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
            passing = f"{last_updated_string},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(pass_first)
            outfile.write(passing)


def UploadfilestTos3():
    try:
        print("uploading files to s3")
        s3 = boto3.client('s3')
        s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"{dag_name}/original/{input_filename}")
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


pdfdownloder("https://fcraonline.nic.in/Home/PDF_Doc/fc_notice_list_19092017.pdf")
fileprocessor()
CompareDocument()
UploadfilestTos3()