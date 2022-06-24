import requests
from scrapy.http import HtmlResponse
import time
import json
import hashlib
from datetime import date, datetime,timedelta
from os.path import exists
import os
import boto3
from deep_translator import GoogleTranslator

#NOTE: Filename according to the date :
out_list = []
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "star-833"
root = "/home/ubuntu/sanctions-scripts/STAR-833/"

# input_filename  = f'{dag_name}-inout-{today_date}.xlsx'
output_filename = f'{dag_name}-output-{today_date}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date}.json'
removed_filename = f'{dag_name}-removed-{today_date}.json'
old_output_filename = f'{dag_name}-output-{yesterday}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
# root = ""
# ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def langtranslation(to_translate):
    try:
        translated = GoogleTranslator(source='auto', target='en').translate(to_translate)
    except:
        try:
            translated = GoogleTranslator(source='auto', target='en').translate(to_translate)
        except:
            print(f">>>Translartion Bug : {to_translate}")
            translated = to_translate  
    return translated

# -*- coding: utf-8 -*-
def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True

def alias_maker(name):
    alias_name = []
    new_name = name.split(" ")
    if len(new_name) == 2:
        alias_name.append(new_name[1]+" "+new_name[0])
    elif len(new_name) == 3 :
        alias_name.append(new_name[2] +" "+ new_name[0] +" "+new_name[1])
    
    return alias_name

def get_hash(n):
    return hashlib.sha256(((n+"Star Health and Allied Insurance Co. Ltd. Black Listed Agents List, India (India Watchlists IND_E20342)").lower()).encode()).hexdigest()

def gender_finder(name):
    n,Gender = "",""
    if name.startswith("Mr."):
        name = name.strip("Mr.").strip()
        Gender = "Male"
    elif name.startswith("MR."):
        name = name.strip("MR.").strip()
        Gender = "Male"
    elif name.startswith("Mr"):
        name = name.strip("Mr").strip()
        Gender = "Male"
    elif name.startswith("Ms."):
        name = name.strip("Ms.").strip()
        Gender = "Female"
    elif name.startswith("Mrs."):
        name = name.strip("Mrs.").strip()
        Gender = "Female"
    return n,Gender


def process_data():
    global out_list,last_updated_string,total_profile_available

    url = "https://www.starhealth.in/irdai-regulations-appointment"
    r = requests.get(url,verify=False)
    resp = HtmlResponse('example.com', body = r.text, encoding = 'utf-8')

    for tr in resp.xpath('//table/tbody/tr'):
        name  = tr.xpath("./td[1]/text()").get(default="")
        pan  = tr.xpath("./td[2]/text()").getall()
        agent_code  = tr.xpath("./td[3]/text()").get(default="")
        office  = tr.xpath("./td[4]/text()").get(default="")
        cdate  = tr.xpath("./td[5]/text()").get(default="")

        if name.startswith("Mr."):
            name = name.strip("Mr.").strip()
            Gender = "Male"
        elif name.startswith("MR."):
            name = name.strip("MR.").strip()
            Gender = "Male"
        elif name.startswith("Mr"):
            name = name.strip("Mr").strip()
            Gender = "Male"
        elif name.startswith("Ms."):
            name = name.strip("Ms.").strip()
            Gender = "Female"
        elif name.startswith("Mrs."):
            name = name.strip("Mrs.").strip()
            Gender = "Female"

        out_list.append({
                'uid': get_hash(name),
                'name':name,
                'alias_name': alias_maker(name),
                'country': ["India"],
                'list_type':"Individual",
                'last_updated':last_updated_string,
                'individual_details':{
                            "gender": Gender
                    },
                'sanction_details':{
                        'agent_id':agent_code,
                        'issue_date': cdate
                        },    
                'nns_status':"False",
                'address':[ 
                            {
                                "complete_address":office,
                                "country":"India"
                            }],
                'document':{"pan":pan},
                'comment':"",   
                'sanction_list':{
                    "sl_authority":"Star Health and Allied Insurance Co. Ltd.",
                    "sl_url":"https://www.starhealth.in/irdai-regulations-appointment",
                    "watch_list":"India Watchlists",
                    "sl_host_country":"India",
                    "sl_type": "Sanctions",
                    "sl_source":"Star Health and Allied Insurance Co. Ltd. Black Listed Agents List, India",
                    "sl_description":"List of agents whose appointment has been cancelled by IRDAI",
                    "list_id":"IND_E20342",  
                    }
            })

    total_profile_available = len(out_list)
    print(f"Total profile available: {total_profile_available}")
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=4,default=str)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=4,default=str)

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
CompareDocument()
UploadfilestTos3() 

        


