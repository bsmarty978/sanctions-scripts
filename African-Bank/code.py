import json
from re import I
import pandas as pd 
import bs4
import hashlib  
import json
import datetime
from datetime import datetime,date,timedelta
import requests
from bs4 import BeautifulSoup
import os
from os.path import exists
import boto3
from copy import deepcopy

#NOTE: Object for output json file
out_list = []
input_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "african-bank"

input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/African-Bank/"
# root = ""
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


r = requests.get(f"https://www.afdb.org/en/projects-operations/debarment-and-sanctions-procedures")
soup = BeautifulSoup(r.content, 'html')


dict_ = {}
dict_['Alias'] =""

def abc(s):
    l1  = s.split("(")
    fi_l = []
    if len(l1) > 1:
        return l1[0].strip()

    else:
        return s

def alias_convert(input_str): 
    l1  = input_str.split("(")
    fi_l = []
    if len(l1) > 1:
        for i in range(1, len(l1)):
            ali  = ""
            if l1[i].startswith("Previously known as"):
                ali=l1[i].split("Previously known as ")[-1].strip().strip(")").strip()
                dict_['Alias'] = ali
            elif l1[i].startswith('"'):
                ali=l1[i].strip('"').strip(")").strip('"').strip()
            elif l1[i].startswith("a.k.a."):
                ali = l1[i].split("a.k.a.")[-1].strip().strip(")").strip()
            elif l1[i].startswith("Formerly known as"):
                ali = l1[i].split("Formerly known as ")[-1].strip(")").strip()
            else:
                ali= l1[i].strip().strip(')').strip('"').strip()

            if ali:
                fi_l.append(ali)

    return fi_l   


def alias_name(name):
    alias_list = []
    subname = name.split(' ')
    l = len(subname)
    if l >= 3:
        name1 = subname[l-1] + " " + subname[0]
        name2 = subname[l-2] + " " + subname[0]
        alias_list.append(name1)
        alias_list.append(name2)
    if l == 2:
        name1 = subname[1] + " " + subname[0]
        alias_list.append(name1)

    return alias_list

def get_hash(n):
    return hashlib.sha256(((n+"African Development Bank Group"+"Debarment and Sanctions Procedures"+"International").lower()).encode()).hexdigest()

def process_data():
    final_list = []

    for table in soup.find_all('div', class_='container-fluid inner-page-content'):
    
        for rows in table.find_all('tr'):
            dict_ = {}
            dict_['Alias'] = ""
            Type = rows.find_all('td', class_='views-field views-field-field-debarment-type')
            for i in Type:
                t = i.text
                dict_['Type'] = t.strip()
                if dict_['Type'] == "FIRM":
                    dict_['Type'] = "Entity"
                else:
                    dict_['Type'] = "Individual"

                
            Name = rows.find_all('td', class_='views-field views-field-title')
            for j in Name:
                dict_['Name'] =  j.text.strip()
            
            
            Nationality = rows.find_all('td', class_='views-field views-field-field-debarment-nationality')
            for k in Nationality:
                dict_['Nationality'] = k.text.strip()
                
            Basis = rows.find_all('td', class_='views-field views-field-field-debarment-basis-for-inelig')
            for l in Basis:
                dict_['Basis'] =  l.text.strip()

            
            final_list.append(dict_)  
        
    final_list.pop(0)
    last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S") 
    record_list = []

    for record in final_list:
        
        try:
            if record['Type'] == "Entity":
                record_list.append(
                            {
                                    "uid": get_hash(record['Name']),
                                    "name": abc(record['Name']),
                                    "alias_name":alias_convert(record['Name']),
                                    "country": [record['Nationality'.strip()]],
                                    "list_type": "Entity",
                                    "last_updated": last_updated_string,
                                    "entity_details": {},
                                    "list_id": "INT_E20182",
                                    "nns_status": "False",
                                    "address": [
                                                    {
                                                        "street": "",
                                                        "city": "",
                                                        "ZIP_code": "",
                                                        "country": record['Nationality'.strip()],
                                                        "complete_address": record['Nationality'.strip()]                                                }
                                                ],
                                    "sanction_Details": {
                                                            "Grounds": record['Basis'],
                                                            "Sanctions Type":""
                                                        },
                                    "documents": {},
                                    "comment": "",
                                    "sanction_list": {
                                                        "sl_authority": "African Development Bank Group",
                                                        "sl_url": "https://www.afdb.org/en/projects-operations/debarment-and-sanctions-procedures",
                                                        "watch_list": "Global Watchlists",
                                                        "sl_host_country": "International",
                                                        "sl_type": "Sanctions",
                                                        "sl_source": "African Development Bank, Debarred Entities and Individuals List",
                                                        "sl_description": "The individuals and firms below have been sanctioned by the African Development Bank Group or by signatories to the Agreement for Mutual Enforcement of Debarment Decisions. Sanctions are imposed on entities found to have participated in coercive, collusive, corrupt, fraudulent or obstructive practices under the Bank’s sanctions system or adopted under the Agreement for Mutual Enforcement of Debarment Decisions."
                                                    }
                            }
            )
            else:
                record_list.append(
                    {
                            "uid": get_hash(record['Name']),
                            "name":abc(record['Name']),
                            "alias_name":alias_convert(record['Name']),
                            "country": [record['Nationality'.strip()]],
                            "list_type": "Individual",
                            "last_updated": last_updated_string,
                            "individual_details":{
                            "date_of_birth":[],
                            "gender":""
                            },
                            "list_id": "INT_E20182",
                            "nns_status": "False",
                            "address": [
                                            {
                                                "street": "",
                                                "city": "",
                                                "ZIP_code": "",
                                                "country": record['Nationality'.strip()],
                                                "complete_address": record['Nationality'.strip()],
                                                
                                            }
                                        ],
                            "sanction_Details": {
                                                    "Grounds":record['Basis'],
                                                    "Sanctions Type":""
                                                },
                            "documents": {
                                                    "passport": [],
                                                    "national_id": [],
                                                    "SSN": ""
                                                },
                            "comment": "",
                            "sanction_list": {
                                                "sl_authority": "African Development Bank Group",
                                                "sl_url": "https://www.afdb.org/en/projects-operations/debarment-and-sanctions-procedures",
                                                "watch_list": "Global Watchlists",
                                                "sl_host_country": "International",
                                                "sl_type": "Sanctions",
                                                "sl_source": "African Development Bank, Debarred Entities and Individuals List",
                                                "sl_description": "The individuals and firms below have been sanctioned by the African Development Bank Group or by signatories to the Agreement for Mutual Enforcement of Debarment Decisions. Sanctions are imposed on entities found to have participated in coercive, collusive, corrupt, fraudulent or obstructive practices under the Bank’s sanctions system or adopted under the Agreement for Mutual Enforcement of Debarment Decisions."
                                                }
                        },
                )
            
        except:
            print(record)   
            break     
    global total_profile_available,out_list
    total_profile_available = len(record_list)
    out_list = deepcopy(record_list)
    print(f"Total Available Profiles : {total_profile_available}")
    #NOTE: There is not any Input FIle to store.
    # try:
    #     with open(f'{ip_path}/{input_filename}', "w",encoding='utf-8') as infile:
    #         json.dump(input_list, infile,ensure_ascii=False,indent=2)
    # except FileNotFoundError:
    #     os.mkdir(ip_path)
    #     with open(f'{ip_path}/{input_filename}', "w",encoding='utf-8') as infile:
    #         json.dump(input_list, infile,ensure_ascii=False,indent=2)       
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