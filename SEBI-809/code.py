# -*- coding: utf-8 -*-


import pandas as pd
import time
import requests
from scrapy.http import HtmlResponse
from datetime import datetime,date,timedelta
import os
from os.path import exists
import json
from copy import deepcopy
import boto3
import copy
import hashlib


"""# SNG-809"""



#NOTE: Filename according to the date :
out_list = []
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "sebi-809"
root = "/home/ubuntu/sanctions-scripts/SEBI-809/"

input_filename1  = f'{dag_name}-inout1.xlsx'
input_filename2  = f'{dag_name}-inout2.xlsx'
output_filename = f'{dag_name}-output-{today_date}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date}.json'
removed_filename = f'{dag_name}-removed-{today_date}.json'
old_output_filename = f'{dag_name}-output-{yesterday}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
# root = ""
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def get_hash(n):
    return hashlib.sha256(((n+"MSE Debarred Entities and Individuals, India").lower()).encode()).hexdigest()


def alias_maker(n):
    na = n.split(" ")
    alis = []
    if len(na)==2:
        alis.append((na[1].strip()+" "+na[0].strip()).strip())
    elif len(na) == 3:
        alis.append((na[2].strip()+" "+na[0].strip()+" "+na[1].strip()).strip())
    # elif len(na)>2:
    #     alis.append((na[1].strip()+" "+na[0].strip()).strip())
    return alis

def sourcedownloader():
    headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
        }

    r =  requests.get("https://www.msei.in/Investors/List-of-Debarred-Entities")
    resp = HtmlResponse("example.com",body=r.text,encoding='utf-8')
    url1 ="https://www.msei.in"+ resp.xpath("(//td[@class='txtcntr t-icon'])[1]/a/@href").get()
    url2 ="https://www.msei.in"+ resp.xpath("(//td[@class='txtcntr t-icon'])[2]/a/@href").get()

    print(url1)
    print(url2)
    # exit()
    res1 = requests.get(url1,headers=headers)
    res2 = requests.get(url1,headers=headers)

    try:
        with open(f'{ip_path}/{input_filename1}', "wb") as infile:
            infile.write(res1.content)
        with open(f'{ip_path}/{input_filename2}', "wb") as infile:
            infile.write(res2.content)
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename1}', "wb") as infile:
            infile.write(res1.content)
        with open(f'{ip_path}/{input_filename2}', "wb") as infile:
            infile.write(res2.content)


def process_data():
    global out_list,last_updated_string,total_profile_available
    df1 = pd.read_excel(f'{ip_path}/{input_filename1}')
    df2 = pd.read_excel(f'{ip_path}/{input_filename2}')
    df1.fillna("",inplace=True)
    df2.fillna("",inplace=True)
    
    frame_ = df1.to_dict(orient="records")
    frame_.extend(df2.to_dict(orient="records"))
    # print(df.columns)
    # exit()

    for each_obj in frame_:
        items = {}
            
        new_alias = each_obj['Client Name'].strip("Mr. ").strip("Mr ").strip("Mrs. ").strip("Mrs ").strip("Ms. ").strip("Ms ").strip()
        
        name_ = each_obj["Client Name"]
        Gender = ""
        if name_.startswith("Mr."):
            name_ = name_.strip("Mr.").strip()
            Gender = "Male"
        elif name_.startswith("MR."):
            name_ = name_.strip("MR.").strip()
            Gender = "Male"
        elif name_.startswith("Mr"):
            name_ = name_.strip("Mr").strip()
            Gender = "Male"
        elif name_.startswith("Ms."):
            name_ = name_.strip("MS.").strip()
            Gender = "Female"
        elif name_.startswith("Ms"):
            name_ = name_.strip("MS").strip()
            Gender = "Female"
        elif name_.startswith("Mrs"):
            name_ = name_.strip("Mrs").strip()
            Gender = "Female"
        items["uid"] = get_hash(name_)
        items['name'] = name_
            
        if name_.endswith("Incorporation"):
            list_type = "Entity"
        elif name_.endswith("CONSULTANCY"):
            list_type = "Entity"
        elif name_.endswith("Corporation"):
            list_type = "Entity"
        elif name_.endswith("Ltd"):
            list_type = "Entity"
        elif name_.endswith("Ltd."): 
            list_type = "Entity"
        elif name_.endswith("Limited"):
            list_type = "Entity"
        elif name_.endswith("Limited."):
            list_type = "Entity"
        elif name_.endswith(")"):
            list_type = "Entity"
        elif name_.endswith("LTD."):
            list_type = "Entity"
        elif name_.endswith("LTD"):
            list_type = "Entity"
        elif name_.endswith("LIMITED"):
            list_type = "Entity"
        elif name_.endswith("Solutions"):
            list_type = "Entity"
        elif name_.endswith("Solution"):
            list_type = "Entity"
        elif name_.startswith("M/s"):
            list_type = "Entity"
        elif name_.startswith("m/s"):
            list_type = "Entity"
        elif name_.startswith("M/S"):
            list_type = "Entity"
        elif name_.endswith("Research"):
            list_type = "Entity"
        elif name_.endswith("RESEARCH"):
            list_type = "Entity"
        elif name_.endswith("Services"):
            list_type = "Entity"
        elif name_.endswith("Service"):
            list_type = "Entity"
        elif name_.endswith("FZE"):
            list_type = "Entity"
        elif name_.endswith("Partners"):
            list_type = "Entity"
        elif name_.endswith("PARTNERS"):
            list_type = "Entity"
        elif name_.endswith("Bank AG"):
            list_type = "Entity"
        elif name_.endswith("Fund"):
            list_type = "Entity"
        elif name_.endswith("FUND"):
            list_type = "Entity"
        elif name_.endswith("AGRO"):
            list_type = "Entity"
        elif name_.endswith("LLP"):
            list_type = "Entity"
        elif name_.endswith("Llp"):
            list_type = "Entity"
        elif name_.endswith("Advisory"):
            list_type = "Entity"
        elif name_.endswith("Investment"):
            list_type = "Entity"
        elif name_.endswith("INVESTMENT"):
            list_type = "Entity"
        elif name_.endswith("Bank Ag"):
            list_type = "Entity"
        elif name_.endswith("Bank Ag: FII"):
            list_type = "Entity"
        elif name_.endswith("Agro"):
            list_type = "Entity"
        elif name_.endswith("Classic"):
            list_type = "Entity"
        elif name_.endswith("Money"):
            list_type = "Entity"
        elif name_.endswith("HUF"):
            list_type = "Entity"
        elif name_.endswith("Huf"):
            list_type = "Entity"
        elif name_.endswith("Consultancy"):
            list_type = "Entity"
        elif name_.endswith("Capital"):
            list_type = "Entity"
        elif name_.endswith("CAPITAL"):
            list_type = "Entity"
        elif name_.endswith("S.A"):
            list_type = "Entity"
        elif name_.endswith("S.A."):
            list_type = "Entity"
        elif name_.endswith("Co."):
            list_type = "Entity"
        elif name_.endswith("Impex"):
            list_type = "Entity"
        elif name_.endswith("IMPEX"):
            list_type = "Entity"
        elif name_.endswith("Trading"):   
            list_type = "Entity"
        elif name_.endswith("TRADING"):
            list_type = "Entity"
        elif name_.endswith("Associates"):
            list_type = "Entity"
        elif name_.endswith("Company"):
            list_type = "Entity"
        elif name_.endswith("Trust"):
            list_type = "Entity"
        elif name_.endswith("TRUST"):
            list_type = "Entity"
        elif name_.endswith("Limite"):
            list_type = "Entity"
        elif name_.endswith("directors"):
            list_type = "Entity"
        elif name_.endswith("Coppertrenzs"):
            list_type = "Entity"
        elif name_.endswith("Commodities"):
            list_type = "Entity"
        elif name_.endswith("COMMODITIES"):
            list_type = "Entity"
        elif name_.endswith("Currencies"):
            list_type = "Entity"
        elif name_.endswith("Enterprises"):
            list_type = "Entity"
        elif name_.endswith("Diamonds"):
            list_type = "Entity"
        elif name_.endswith("Jewels"):
            list_type = "Entity"
        elif name_.endswith("Securities"):
            list_type = "Entity"
        elif name_.endswith("Developers"):
            list_type = "Entity"
        elif name_.endswith("International"):
            list_type = "Entity"
        elif name_.endswith("Industries"):
            list_type = "Entity"
        elif name_.endswith("Cliff"):
            list_type = "Entity"
        elif name_.endswith("Ceramics"):
            list_type = "Entity"
        elif name_.endswith("Enterprise"):
            list_type = "Entity"
        elif name_.endswith("Tools"):
            list_type = "Entity"
        else:
            list_type = "Individual"
        
        
        items['alias_name'] = []
        if list_type == "Individual":
            items['alias_name'] = alias_maker(new_alias)
        else:
            pass    
            
            
        items['documents'] = {}
        items['documents']['PAN'] = []
        pan = each_obj['PAN No.'].strip()
        if pan:
            items['documents']['PAN'].append(pan)

        
        
        if list_type.startswith("Entity"):
            list_type = "Entity"
            items['entity_details'] = {}
        else:
            list_type = "Individual"
            if list_type == "Individual":
                items['individual_details'] = {}
                items['individual_details']['gender'] = Gender 
            
            
        items['country'] = ["India"]
        items['list_type'] = list_type
        items['last_updated'] = last_updated_string
        items['family-tree'] = {}
        items['sanction_details'] = {}
        items['sanction_details']['imposed_date'] = each_obj['Date of Regulator Order']
        items['sanction_details']['order_no'] = each_obj['Regulator Order No.']
        items['sanction_details']['circular_no'] = each_obj['Exchange Circular No.']
        items['address'] = [{
            "complete_address" : "",
            "country" : "India"
        }]
        items['comment'] = each_obj['Reason']
        items['sanction_list'] = {
            "sl_authority": "Metropolitan Stock Exchange of India Limited, India",
            "sl_url": "https://www.msei.in/Investors/List-of-Debarred-Entities",
            "sl_host_country": "India",
            "sl_type": "Sanctions",
            "watch_list": "India Watchlists",
            "sl_source": "MSE Debarred Entities and Individuals, India",
            "sl_description": "List of individuals and organizations declared defaulter by Metropolitan Stock Exchange of India Limited, India",
            "list_id": "IND_E20324"
        }
        out_list.append(items)
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
            passing = f"{last_updated_string},{input_filename1},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(passing)
    else:
        with open(f'{lp_path}',"a") as outfile:
            pass_first = "date,inputfile,outputfile,total_profile_availabe,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
            passing = f"{last_updated_string},{input_filename1},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(pass_first)
            outfile.write(passing)

def UploadfilestTos3():
    try:
        print("uploading files to s3")
        s3 = boto3.client('s3')
        s3.upload_file(f'{ip_path}/{input_filename1}',"sams-scrapping-data",f"{dag_name}/original/{input_filename1}")
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

sourcedownloader()
process_data()
CompareDocument()
UploadfilestTos3() 

        

