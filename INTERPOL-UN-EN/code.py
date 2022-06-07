import traceback
import requests
import json
from datetime import datetime,date,timedelta
from scrapy.http import HtmlResponse
import os
from os.path import exists
import pandas as pd
import time as t
import boto3
import hashlib
import traceback


import pycountry
countries = {}
for country in pycountry.countries:
    countries[country.alpha_2] = country.name


#NOTE: Filename according to the date :
out_list = []
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "interpol-un-en"
root = "/home/ubuntu/sanctions-scripts/INTERPOL-UN-EN/"

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

def get_hash(n):
    return hashlib.sha256(((n+"Interpol-United Nations Security Council Special Notices for Entities INT_T30094").lower()).encode()).hexdigest()

def getprofile(eid):
    try:
        url = f"https://ws-public.interpol.int/notices/v1/un/entities/{eid}"
        r = requests.get(url)
        data =  json.loads(r.text)
        return data
    except:
        print(f"GetProfile Error : {traceback.print_exc()}")
        return {}


def process_data():
    global out_list,last_updated_string,total_profile_available,countries
    # resp = HtmlResponse('example.com',body=requests.get("https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals#").text,encoding='utf-8')
    # li = resp.xpath("//*[@id='unResolution']//option/@value").getall()

    uval = []
    url = f"https://ws-public.interpol.int/notices/v1/un/entities?&unResolution=&resultPerPage=160"
    r =  requests.get(url)
    data = json.loads(r.text)
    for k in data["_embedded"]["notices"]:
        pass_eid =  k["entity_id"].replace("/","-")
        if pass_eid not in uval:
            uval.append(pass_eid)
    # for i in li:
    #     print(i)
    #     url = f"https://ws-public.interpol.int/notices/v1/un/persons?&unResolution={i}&resultPerPage=160"
    #     r =  requests.get(url)
    #     data = json.loads(r.text)
    #     if data["total"] > 160:
    #         for j in range(10):
    #             surl = f"https://ws-public.interpol.int/notices/v1/un/persons?&unReference={j}&unResolution=1267&resultPerPage=160"
    #             sr =  requests.get(surl)
    #             sdata = json.loads(sr.text)
    #             for k in sdata["_embedded"]["notices"]:
    #                 pass_eid =  k["entity_id"].replace("/","-")
    #                 if pass_eid not in uval:
    #                     uval.append(pass_eid)
    #     else:

    #         for k in data["_embedded"]["notices"]:
    #             pass_eid =  k["entity_id"].replace("/","-")
    #             if pass_eid not in uval:
    #                 uval.append(pass_eid)


    for eid in uval:
        print(eid)
        d = {
        "uid": "",
        "name": "",
        "alias_name": [],
        "country": [],
        "list_type": "Entity",
        "last_updated": last_updated_string,
        "entity_details": {},
        "nns_status": "False",
        "address": [],
        "sanction_details":{
            "referance_no" : "",
            "issue_date" : ""
        },
        "relationship_details" : {
            "associate" :[]
        },
        "documents": {},
        "comment": "",
        "sanction_list": {
            "sl_authority": "INTERPOL",
            "sl_url": "https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Entities",
            "sl_host_country": "United States of America",
            "sl_type": "Sanctions",
            "sl_source": "Interpol-United Nations Security Council Special Notices for Entities",
            "sl_description": "The INTERPOL-United Nations Security Council Special Notice alerts global police to individuals and entities that are subject to sanctions imposed by the United Nations Security Council. The three most common sanctions are assets freeze, travel ban and arms embargo..",
            "watch_list": "Global Watchlists",
            "list_id": "INT_T30094"
        }
            }

        idata = getprofile(eid)
        if idata=={}:
            continue
        
        alias = []
        raw_name =  idata["name"] if idata["name"] else ""
        oname =  idata["name_in_original_script"] if idata["name_in_original_script"] else ""
        also_name =  idata["also_known_as"] if idata["also_known_as"] else ""
        fname =  idata["formerly_known_as"] if idata["formerly_known_as"] else ""

        if raw_name:
            s = raw_name[raw_name.find("(")+1:raw_name.find(")")]
            if s:
                alias.append(s)
                name = raw_name.replace(f'({s})',"").strip()
            else:
                name = raw_name
        else:
            continue

        if also_name:
            alias.extend(also_name.split(";"))
        if fname:
            alias.extend(fname.split(";"))
        if oname:
            alias.append(oname)

        associates = []
        for aso in (idata["associates"] if idata["associates"] else []):
            if aso["name"]:
                associates.append(aso["name"])

        adrs = []
        for j in (idata["adresses"] if idata["adresses"] else []):
            adrs.append({
                "complete_address" : j,
                "country" : ""
            })

        comment = idata["summary"] if idata["summary"] else ""
        un_ref_no = idata["un_reference"] if idata["un_reference"] else ""
        un_ref_date = idata["un_reference_date"] if idata["un_reference_date"] else ""
        
        d["uid"] = get_hash(name+eid)
        d["name"] = name
        ali = []
        for al in alias:
            ali.append(al.replace("F.K.A.:","").strip())
        ali = list(set(ali))
        d["alias_name"] = ali
        d["address"] = adrs
        d["sanction_details"]["referance_no"] = un_ref_no
        d["sanction_details"]["issue_date"] = un_ref_date
        d["relationship_details"]["associate"] = associates
        d["comment"] = comment
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
            passing = f"{last_updated_string}{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(passing)
    else:
        with open(f'{lp_path}',"a") as outfile:
            pass_first = "date,outputfile,total_profile_availabe,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
            passing = f"{last_updated_string}{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
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