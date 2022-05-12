import pandas as pd
from datetime import datetime,timedelta,date
import hashlib
import json
import requests
import os
from os.path import exists
import boto3
from scrapy.http import HtmlResponse

#NOTE: Filename according to the date :
out_list = []
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "nazk-en"

# input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.csv'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/NAZK-EN/"
# root = ""
# ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def get_hash(n, a):
    return hashlib.sha256(((n+"Ministry of Foreign Affairs, National Agency on Corruption Prevention Potential Sanctions Subjects, Ukraine"+a).lower()).encode()).hexdigest()


def process_data():
    global out_list,last_updated_string,total_profile_available
    url = "https://sanctions.nazk.gov.ua/en/sanction-company-warning/"
    req = requests.get(url)
    res = HtmlResponse('example.com', body=req.text, encoding="utf-8")
    last_p = int(res.xpath("//li[@class='last']/a/@data-page").get()) + 1

    p =1
    while(True):
        if p == last_p+1:
            break
        url = f"https://sanctions.nazk.gov.ua/en/sanction-company-warning/?page={p}"
        req = requests.get(url)
        res = HtmlResponse('example.com', body=req.text, encoding="utf-8")
        for i in res.xpath("//div[@class='row']"):
            item = {} 
            id_ = i.xpath("./div[@class='id']/text()").get()
            name = i.xpath("./div[@class='name']/span/text()").get()
            iden = i.xpath("./div[@class='center']/text()").getall()
            tin = iden[0]
            srn = iden[1]

            item['uid'] = get_hash(name, id_)
            item['name'] = name
            item['alias'] = []
            item['country'] = []
            item['list_type'] = "Entity"
            item['last_updated'] = last_updated_string
            item['entity_details'] = {}
            item['documents'] = {}
            item['documents']["TIN"] = [tin]
            item['documents']['SRN'] = [srn]
            item['sanction_details'] = []
            item['nns_status'] = "False"
            item['address'] = {}
            item['comment'] = ""
            item['sanction_details'] = {}
            item['sanction_list'] = {}
            item['sanction_list']['sl_authority'] = "National Agency on Corruption Prevention, Ministry of Foreign Affairs, Ukraine"
            item['sanction_list']['sl_url'] = "https://sanctions.nazk.gov.ua/"
            item['sanction_list']['watch_list'] = "European Watchlists"
            item['sanction_list']['sl_type'] = "Potential Sanctions"
            item['sanction_list']['sl_source'] = "Ministry of Foreign Affairs, National Agency on Corruption Prevention Potential Sanctions Subjects, Ukraine"
            item['sanction_list']['sl_source_url'] = "https://sanctions.nazk.gov.ua/en/sanction-company-warning/"
            item['sanction_list']['sl_description'] = "List of individuals and organizations who are potential sanctions subject by National Agency on Corruption Prevention, Ministry of Foreign Affairs, Ukraine"
            item['sanction_list']['list_id'] = "UKR_E20299"
            
            out_list.append(item)
        p +=1
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
            passing = f"{last_updated_string},,{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
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