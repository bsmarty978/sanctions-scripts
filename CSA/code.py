from playwright.sync_api import sync_playwright
from scrapy.http import HtmlResponse
import time
import json
import os
from os.path import exists
import boto3
from copy import deepcopy
from datetime import datetime,date,timedelta
import hashlib

#NOTE: Object for output json file
out_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "CSA"

# input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/CSA/"
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

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        url = "https://info.securities-administrators.ca/disciplinedpersons.aspx"
        page.goto(url)
        print(page.title())

        # NOTE: Show all
        page.click("//input[@id='bodyContent_ibtn_search']")
        page.wait_for_selector("//table[@id='bodyContent_gv_list']")

        cond = []
        while True:
            resp = HtmlResponse("example.com",body=page.content(),encoding='utf-8')
            names = resp.xpath("//table[@id='bodyContent_gv_list']/tbody/tr/td/a/span/text()").getall()
            dates = resp.xpath("//table[@id='bodyContent_gv_list']/tbody/tr/td[2]/span/text()").getall()
            ltypes = resp.xpath("//table[@id='bodyContent_gv_list']/tbody/tr/td[3]/span/text()").getall()

            names.sort()
            if cond == names:
                print("completed")
                break
            else:
                cond = names
                for i in range(len(names)):
                    name = names[i].replace(",","").strip()
                    uid = get_hash(name)
                    odate = dates[i].strip()
                    list_type = ltypes[i].strip()
                    if list_type == "Person":
                        list_type = "Individual"
                        out_list.append({
                            "uid":uid,
                            "name": name,
                            "alias_name":[],
                            "country": [],
                            "list_type": "Individual",
                            "last_updated": last_updated_string,
                            "list_id": "CAN_T30029",
                            "individual_details": {},
                            "nns_status": "False",
                            "address": [
                                            {
                                                "country": "",
                                                "complete_address": ""
                                            }
                                        ],
                            "sanction_details": {"date" : odate},
                            "documents": {},
                            "comment":"",
                            "sanction_list": {
                                    "sl_authority": "Canadian Securities Administrators, Canada",
                                    "sl_url": "https://info.securities-administrators.ca/disciplinedpersons.aspx",
                                    "watch_list": "North America Watchlists",
                                    "sl_host_country": "Canada",
                                    "sl_type": "Sanctions",
                                    "sl_source": "Canadian Securities Administrators ‐ Disciplined (fined) Persons List, Canada",
                                    "sl_description": "List of individuals and entities fined by Canadian Securities Administrators, Canada"
                                }
                        }
                        )
                    else:
                        list_type = "Entity"
                        out_list.append({
                            "uid":uid,
                            "name": name,
                            "alias_name":[],
                            "country": [],
                            "list_type": "Entity",
                            "last_updated": last_updated_string,
                            "list_id": "CAN_T30029",
                            "entity_details": {},
                            "nns_status": "False",
                            "address": [
                                            {
                                                "country": "",
                                                "complete_address": ""
                                            }
                                        ],
                            "sanction_details": {"date" : odate},
                            "documents": {},
                            "comment":"",
                            "sanction_list": {
                                    "sl_authority": "Canadian Securities Administrators, Canada",
                                    "sl_url": "https://info.securities-administrators.ca/disciplinedpersons.aspx",
                                    "watch_list": "North America Watchlists",
                                    "sl_host_country": "Canada",
                                    "sl_type": "Sanctions",
                                    "sl_source": "Canadian Securities Administrators ‐ Disciplined (fined) Persons List, Canada",
                                    "sl_description": "List of individuals and entities fined by Canadian Securities Administrators, Canada"
                                }
                        }
                        )
                    

                #NOTE: Next Page
                page.click("//a[@id='bodyContent_lbtnNext2']")
                page.wait_for_selector("//table[@id='bodyContent_gv_list']")
                time.sleep(1)

            print(names[0])
        browser.close()

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
# UploadfilestTos3()