import encodings
import requests
from scrapy.http import HtmlResponse
from datetime import datetime,date,timedelta
import json
import hashlib
import os
from os.path import exists
from copy import deepcopy
import boto3

#NOTE: Filename according to the date :
out_list = []
out_dict = {}
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "SNG-900"
root = "/home/ubuntu/sanctions-scripts/SNG-900/"

# input_filename  = f'{dag_name}-inout-{today_date}.csv'
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


def get_hash(n):
    return hashlib.sha256(((n+"USA Patriot Act Saction 311 Special Measures List, USA (North America Watchlists USA_S10055)").lower()).encode()).hexdigest()


def process_data():
    global out_list,last_updated_string,total_profile_available

    url = "https://www.fincen.gov/resources/statutes-and-regulations/311-special-measures"

    # headers = {
    #     'Cookie': 'PHPSESSID=af56ab0690cba426b0cc6c15027b89ab; TS01b6fa6b=01885b66e8083558d8eaf949ade0cfcf75a46c3a69088d1f4d4449f9cc99a84c036c294521c0b623e42f2fa621ca98fce3e457dadce5be3b1125df83ae024650a0806c8139'
    # }

    res = requests.get(url)
    response = HtmlResponse(url="example.com",body=res.content,encoding='utf-8')

    for tr in  response.xpath('//tbody/tr'):
        name = tr.xpath("normalize-space(./td[1]/text())").get("").strip()
        name = name.replace("*","").strip()
        if not name:
            continue
        ali = []
        if ";" in name:
            nspl = [i.strip() for i in name.split(";")]
            name = nspl[0]
            ali.extend(nspl[1:])
        if "(" in name:
            al = name[name.find("(")+1:name.find(")")]
            ali.append(al)
            name = name.replace(f"({al})","").strip()
        
        idate = tr.xpath("normalize-space(./td[3]/a/text())").get("").strip()

        out_list.append({
            "uid": get_hash(name),
            "name": name,
            "alias_name": ali,
            "country": [],
            "list_type": "Entity",
            "last_updated": last_updated_string,
            "entity_details": {},
            "nns_status": False,
            "address": [
                {
                    "complete_address": "",
                    "country": ""
                }
            ],
            "documents": {},
            "comment": "",
            "sanction_details": {
                "issue_date": idate
            },
            "sanction_list": {
                "sl_authority": "The Financial Crimes Enforcement Network (FinCEN), USA",
                "sl_url": "https://www.fincen.gov/resources/statutes-and-regulations/311-special-measures",
                "sl_host_country": "USA",
                "sl_type": "Sanctions",
                "sl_source": "USA Patriot Act Special Measures List, USA",
                "sl_description": "Special Measures for Jurisdictions, Financial Institutions, or International Transactions of Primary Money Laundering Concerns issued by The Financial Crimes Enforcement Network (FinCEN), USA.",
                "watch_list": "North America Watchlists",
                "list_id": "USA_S10055"
            }}
        )

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
CompareDocument()
UploadfilestTos3() 
