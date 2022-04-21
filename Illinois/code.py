from bs4 import BeautifulSoup
from datetime import datetime,date,timedelta
import hashlib
import json
import requests
import os
from os.path import exists
import boto3
from copy import deepcopy

#NOTE: Object for output json file
out_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "illinois"

# input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/Illinois/"
# root = ""
# ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def get_hash(n):
    return hashlib.sha256(((n+"Prohibited Investment List").lower()).encode()).hexdigest()    
            

def process_data():
    global total_profile_available,out_list
    r = requests.get("https://iipb.illinois.gov/prohibited-investment-list.html")
    soup = BeautifulSoup(r.content, 'lxml')
    name_list = []
    first_div = soup.find_all('div', id='text-fd78cf708b')
    for i in first_div:
        new_name = i.text.strip()
        nn = new_name.split('\n')
        for each in nn:
            dict_ = {}
            if each:
                dict_['name'] = each.strip()
                name_list.append(dict_)
            else:
                pass
            
    second_div = soup.find_all('div', id='text-2426b1469a')
    for j in second_div:
        new_name_1 = j.text.strip()
        mm = new_name_1.split('\n')
        for k in mm:
            dict_ = {}
            if k:
                dict_['name'] = k.strip()
                name_list.append(dict_)            
            else:
                pass
    name_list.pop()       

    third_div = soup.find_all('div', id='text-e2291d7cdf')
    for l in third_div:
        new_name_2 = l.text.strip()
        ee = new_name_2.split('\n')
        for m in ee:
            dict_ = {}
            if m:
                dict_['name'] = m.strip()
                name_list.append(dict_)            
            else:
                pass
    name_list.pop()  

    fourth_div = soup.find_all('div', id='text-65f9335444')
    for o in fourth_div:
        new_name_3 = o.text.strip()
        ff = new_name_3.split('\n')
        for n in ff:
            dict_ = {}
            if n:
                dict_['name'] = n.strip()
                name_list.append(dict_)            
            else:
                pass
    name_list.pop()     
    name_list.pop()

    for rec in name_list:
        out_list.append(
                            {
                                "uid":get_hash(rec['name']),
                                    "name":rec['name'],
                                    "alias_name":[],
                                    "country": "",
                                    "list_type": "Entity",
                                    "last_updated": last_updated_string,
                                    "list_id": "USA_E20290",
                                    "entity_details": {},
                                    "designation": [],
                                    "nns_status": "False",
                                    "address": [
                                                    {
                                                        "country": "",
                                                        "complete_address": ""
                                                    }
                                                ],
                                    "sanction_Details": {
                                                            "Grounds": "",
                                                            "Sanctions Type":""
                                                        },
                                    "documents": {},
                                    "comment":"",
                                    "sanction_list": {
                                                        "sl_authority": "Illinois Investment Policy Board, USA",
                                                        "sl_url": "https://iipb.illinois.gov/prohibited-investment-list.html",
                                                        "watch_list": "North America Watchlists",
                                                        "sl_host_country": "USA",
                                                        "sl_type": "Sanctions",
                                                        "sl_source": "Illinois Investment Policy Board, Prohibited Investment List, USA",
                                                        "sl_description": "List of the Companies which have been deemed prohibited from investment through laws set forth in Article 1 of the Illinois Pension Code and approved by the Illinois Investment Policy Board., USA"
                                                    }
                            }
        )
        
    total_profile_available = len(out_list)
    print(f"Total Available Profile : {total_profile_available}")
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
CompareDocument()
UploadfilestTos3()