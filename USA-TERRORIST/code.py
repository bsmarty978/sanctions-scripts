from bs4 import BeautifulSoup
from datetime import datetime,date,timedelta
import hashlib
import json
import requests
import os
from os.path import exists
import boto3
from copy import deepcopy
from lxml import etree

#NOTE: Object for output json file
out_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "usa-terrorist"

# input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/USA-TERRORIST/"
# root = ""
# ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def alias_name(name):
    out = []
    name_comma = name.split('(')
    if len(name_comma)>1 and len(name_comma) <=2:
        name_2 = name_comma[1].split(';')
        for each in name_2:
            str1 = each.replace('a.k.a.', "")
            str1 = str1.replace('f.k.a.', "")
            str1 = str1.strip().strip(')')
            out.append(str1)
    elif len(name_comma) > 2:
            name_2 = name_comma[1].split(';')
    else:
        out.append(name)
    return out
        
def name_generator(z):
    nm = z.split('(')
    new_name = nm[0].strip()
    return new_name


def get_hash(n):
    return hashlib.sha256(((n+"Terrorist Exclusion List").lower()).encode()).hexdigest()    
    
def process_data():
    global total_profile_available,out_list
    r = requests.get("https://www.state.gov/terrorist-exclusion-list/")
    xp = "//div[@class='entry-content']/ul[position()>=3]/li/text()"
    soup = BeautifulSoup(r.content, 'html.parser')
    dom = etree.HTML(str(soup))
    final_list = dom.xpath(xp)

    for rec in final_list:
        if rec == 'Revival of Islamic Heritage Society (Pakistan and Afghanistan offices — Kuwait office not designated) (a.k.a. Jamia Ihya ul Turath; a.k.a. Jamiat Ihia Al- Turath Al-Islamiya; a.k.a. Revival of Islamic Society Heritage on the African Continent)':
            rec = 'Revival of Islamic Heritage Society (a.k.a. Jamia Ihya ul Turath; a.k.a. Jamiat Ihia Al- Turath Al-Islamiya; a.k.a. Revival of Islamic Society Heritage on the African Continent; Pakistan and Afghanistan offices — Kuwait office not designated)'
        if rec == 'The Islamic International Brigade (a.k.a. International Battalion, a.k.a. Islamic Peacekeeping International Brigade, a.k.a. Peacekeeping Battalion, a.k.a. The International Brigade, a.k.a. The Islamic Peacekeeping Army, a.k.a. The Islamic Peacekeeping Brigade)':
            rec = 'The Islamic International Brigade (a.k.a. International Battalion; a.k.a. Islamic Peacekeeping International Brigade; a.k.a. Peacekeeping Battalion; a.k.a. The International Brigade; a.k.a. The Islamic Peacekeeping Army; a.k.a. The Islamic Peacekeeping Brigade)'
        if rec == 'The Riyadus-Salikhin Reconnaissance and Sabotage Battalion of Chechen Martyrs (a.k.a. Riyadus-Salikhin Reconnaissance and Sabotage Battalion, a.k.a. Riyadh-as-Saliheen, a.k.a. the Sabotage and Military Surveillance Group of the Riyadh al-Salihin Martyrs, a.k.a. Riyadus-Salikhin Reconnaissance and Sabotage Battalion of Shahids (Martyrs))':
            rec = 'The Riyadus-Salikhin Reconnaissance and Sabotage Battalion of Chechen Martyrs (a.k.a. Riyadus-Salikhin Reconnaissance and Sabotage Battalion; a.k.a. Riyadh-as-Saliheen; a.k.a. the Sabotage and Military Surveillance Group of the Riyadh al-Salihin Martyrs; a.k.a. Riyadus-Salikhin Reconnaissance and Sabotage Battalion of Shahids; Martyrs)'
        
        out_list.append(
                    {
                                    "uid":get_hash(rec),
                                    "name":name_generator(rec),
                                    "alias_name":alias_name(rec),
                                    "country": "",
                                    "list_type": "Entity",
                                    "last_updated": last_updated_string,
                                    "list_id": "USA_T30039",
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
                                                        "sl_authority": "Bureau of Counterterrorism, U.S. Department of State, USA",
                                                        "sl_url": "https://www.state.gov/terrorist-exclusion-list/",
                                                        "watch_list": "North America Watchlists",
                                                        "sl_host_country": "USA",
                                                        "sl_type": "Sanctions",
                                                        "sl_source": "US Terrorist Exclusion List, USA",
                                                        "sl_description": "List of the Terrorists Organization by Bureau of Counterterrorism, U.S. Department of State, USA"
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