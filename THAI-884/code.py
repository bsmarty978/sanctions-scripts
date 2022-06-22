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

dag_name = "thai-884"
root = "/home/ubuntu/sanctions-scripts/THAI-884/"

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

def get_hash(n):
    return hashlib.sha256(((n+"The Office of the Securities and Exchange Commission of Thailand Investor Alert List, Thailand (APAC Watchlists THA_E20364)" + "THA_E20364").lower()).encode()).hexdigest()
  
def process_data():
    global out_list,last_updated_string,total_profile_available

    url = "https://market.sec.or.th/public/idisc/en/Viewmore/invalert-head?PublicFlag=Y"
    r = requests.get(url)
    resp = HtmlResponse('example.com', body = r.text, encoding = 'utf-8')

    for tr in resp.xpath('//table//tr'):
        data_dict = {}
        name = str(tr.xpath('./td[1]/text()').get()).lower()
        if not name or not name.strip():
            continue
        if 'website' in  name:
            name = name.split('website')[0].replace('https://','').strip()
        elif '(' in name:
            name = str(name.split('(')[0]).replace('https://','').strip()
        alias = []
        uid = get_hash(name)
        disc_date = tr.xpath('./td[3]/text()').get()
        comment = tr.xpath('./td[2]/text()').get()
        website = str(tr.xpath('./td//ul/li[1]/text()').get()).lower()
        website = website.replace('website : ','').strip()
        web_pass = []
        
        if website== "n/a":
            website = ''
        else:
            if ',' in website:
                for j in website.split(','):
                    web_pass.append(j.strip())
            else:
                web_pass.append(website)
        address = str(tr.xpath('./td//ul/li[4]/text()').get()).lower()
        address = address.replace('address : ','').strip()
        if address== "n/a":
            address = ''
        data_dict['uid'] = uid
        if not isEnglish(name):
            alias.append(name)
            name = langtranslation(name)
        data_dict['name'] = name
        data_dict['alias_name'] = alias
        data_dict['list_type'] = 'Entity'
        data_dict['last_updated'] = last_updated_string
        data_dict['country']=[]
        data_dict['nns_status'] = 'False'
        data_dict['sanction_details'] = {'issue_date':disc_date}
        data_dict['entity_details'] = {'website':web_pass}
        data_dict['address'] = [{'complete_addrss':address,'country':''}]
        data_dict['comment'] = comment
        data_dict["sanction_list"]= {
                "sl_authority": "The Office of the Securities and Exchange Commission, Thailand",
                "sl_url": "https://market.sec.or.th/public/idisc/en/Viewmore/invalert-head?PublicFlag=Y",
                "sl_host_country": "Thailand",
                "sl_type": "Sanctions",
                "watch_list": "APAC Watchlists",
                "sl_source": "The Office of the Securities and Exchange Commission of Thailand Investor Alert List, Thailand",
                "sl_description": "Alerts and List of False entities shared by The Office of the Securities and Exchange Commission, Thailand.",
                "list_id": "THA_E20364"
            }

        if name!="none" and data_dict:
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

        


