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

dag_name = "reliance-847"
root = "/home/ubuntu/sanctions-scripts/Reliance-847/"

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
    return hashlib.sha256(((n+"SReliance Life Insurance Co. Ltd. Black Listed Agents List, India (India Watchlists IND_E20356)").lower()).encode()).hexdigest()

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

    url = "https://www.reliancenipponlife.com/handlers/get_Details_of_Terminated_Advisors.ashx"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36 Edg/102.0.1245.33","cookie": "ASP.NET_SessionId=h2lhrhetjs2tghlg0wh2vgra; SC_ANALYTICS_GLOBAL_COOKIE=ee5a871126d5485a822eac3007f50b7b|False; MAScookie=!dIhRyMFhuUiVpiKMUcAcxB4uQg+8eQJK36TNFZZkDdG1F5VpHxDfW+o5TAGIc1M9z2feFILOTq7Ztt779seIU2ZCcoI0WQu9NUDZeu1vwxuWonZFSiLq0w+Ha8Gdx10KZRdSjsy1jb4p5AUFsS8Q2F9NAKeiikA=; TS01dd65cc=01df21a10a922522902721bc0e53dd34caf6a0df1b727d55eaadc2ab8e038e4bab74abae023af7c4ca89e7b372c62c67c30bf8846635925a6e0b8900f42b40dbf3da5152cce0be78ef88ced5efc72058a5df5384c0b40ae00dd81c4f7b99ae5e6ff89679e3; _ga=GA1.3.1251844780.1654762359; _gid=GA1.3.1438920284.1654762359; _hjMinimizedPolls=488055; _sp_id.a65f=c1ac2f71-dca1-4ccd-a7d1-e0fa0abced7a.1654762360.1.1654762396.1654762360.82ce2841-66c1-4c27-adbe-0b9fdf927f2e; _hjSessionUser_756852=eyJpZCI6ImRhNzZlOGEyLWVkNTEtNThhZC1hMTI1LWVhOWFmNDZhNDczYyIsImNyZWF0ZWQiOjE2NTQ3NjIzNjAzNjEsImV4aXN0aW5nIjp0cnVlfQ=="
        }
    r = requests.get(url, headers= headers)
    resp = HtmlResponse("example.com",body=r.text,encoding='utf-8')

    for ul in resp.xpath('//ul'):
        data_dict = {}
        name = ul.xpath('./li[3]/text()').get("")
        name = name.strip()
        if name.lower() == 'agent name' or name == "":
            continue
        agent_code = ul.xpath('./li[2]/text()').get("")
        license_no = ul.xpath('./li[5]/text()').get("")
        issue_date = ul.xpath('./li[7]/text()').get("")    
        reason = ul.xpath('./li[9]/text()').get("")
        reason.strip()
        data_dict['uid'] = get_hash(name)
        data_dict['name'] = name
        data_dict['alias_name'] = alias_maker(name)
        data_dict['list_type'] = 'Individual'
        data_dict['last_updated'] = last_updated_string
        data_dict['nns_status'] = 'False'
        data_dict['sanction_details'] = {'issue_date':issue_date}
        data_dict['individual_details'] = {}
        data_dict['country']=['India']
        data_dict['address'] = [{'complete_addrss':'','country':'India'}]
        data_dict['documents'] = {'license_no': [license_no.strip()],'agent_code':agent_code.strip()}
        data_dict['comment'] = reason
        data_dict["sanction_list"]= {
                    "sl_authority": "Reliance Life Insurance Co. Ltd., India",
                    "sl_url": "https://www.reliancenipponlife.com/public-disclosures",
                    "sl_host_country": "India",
                    "sl_type": "Sanctions",
                    "watch_list": "India Watchlists",
                    "sl_source": "Reliance Life Insurance Co. Ltd. Black Listed Agents List, India",
                    "sl_description": "List of blacklisted agents by Reliance Life Insurance Co. Ltd.",
                    "list_id": "IND_E20356"
                }
        out_list.append(data_dict)

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

        


