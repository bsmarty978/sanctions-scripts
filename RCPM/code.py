from bs4 import BeautifulSoup
import json
import datetime
from datetime import datetime,date,timedelta
import hashlib
import requests
import os
from os.path import exists
import boto3
from copy import deepcopy


#NOTE: Object for output json file
out_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "RCPM"
root = "/home/ubuntu/sanctions-scripts/RCPM/"

# input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
# root = ""
# ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def get_hash(n):
    return hashlib.sha256(((n+"Royal Canadian Mounted Police Wanted List, Canada" + "CAN_T30021").lower()).encode()).hexdigest()    
    

def alias_name(name):
    out = []
    str1 = name.replace("/", ",")
    str1.strip()
    str2 = str1.split(',')
    
    for each_element in str2:
        out.append(each_element.strip())
    return out


def parsing():
    r = requests.get('https://www.rcmp-grc.gc.ca/en/wanted')
    soup = BeautifulSoup(r.content, 'lxml')

    all_urls = []

    s1 = soup.find_all('div', class_='view-content row')

    for i in s1:
        for each in i.find_all('a', href=True):
            all_urls.append("https://www.rcmp-grc.gc.ca/en/"+each['href'])
            

    final_list = []
    for each_link in all_urls:
        r2 = requests.get(each_link)
        soup2 = BeautifulSoup(r2._content, 'lxml')
        
        dict_ = {}
        name = soup2.find('h1', class_="page-header mrgn-tp-md").text.strip()
        print(name)
        dict_['name'] = name
        
        comments = []
        s2 = soup2.find_all('div', class_="col-md-9 col-sm-8")
        for p_tag in s2:
            for d in p_tag.find_all('p'):
                comments.append(d.text)
        
        c = ""
        for i in comments:
            c = c + i.strip()
        dict_['comment'] = c
        

        raw_data = {}
        s3 = soup2.find_all('ul', class_="colcount-md-2 mrgn-tp-md list-unstyled")
        for a in s3:
            for b in a.find_all('li'):
                data = b.text
                new_data =  data.split(':')
                # print(new_data)
                try:
                    raw_data[new_data[0]] = new_data[1]
                except:
                    pass
        # print(raw_data)
        
        try:
            dict_['alias'] = raw_data['Aliases'].strip()
        except:
            dict_['alias'] = dict_['name']
        
        dict_['gender'] = raw_data['Sex'].strip()
        
        try:
            dict_['date_of_birth'] = raw_data['Born'].strip()
            # print(dict_['date_of_birth'])
        except:
            dict_['date_of_birth'] = ""
        
        final_list.append(dict_)
                    

    for each_record in final_list:
        out_list.append(
                        {
                                    "uid":get_hash(each_record['name']),
                                    "name": each_record['name'],
                                    "alias_name":alias_name(each_record['alias']),
                                    "country": "",
                                    "list_type": "Individual",
                                    "last_updated": last_updated_string,
                                    "list_id": "CAN_T30021",
                                    "individual_details": {
                                                            "date_of_birth": [each_record['date_of_birth']],
                                                            "gender": each_record['gender'],
                                                            },
                                    "nns_status": "False",
                                    "address": [
                                                    {
                                                        "country": "",
                                                        "complete_address": ""
                                                    }
                                                ],
                                    "sanction_Details": {},
                                    "documents": {},
                                    "comment": each_record['comment'],
                                    "sanction_list": {
                                                        "sl_authority": "Royal Canadian Mounted Police, Canada",
                                                        "sl_url": "https://www.rcmp-grc.gc.ca/en/wanted",
                                                        "watch_list": "North America Watchlists",
                                                        "sl_host_country": "CANADA",
                                                        "sl_type": "Sanctions",
                                                        "sl_source": "Royal Canadian Mounted Police Wanted List, Canada",
                                                        "sl_description": "List of Wanted Criminals by The Royal Canadian Mounted Police, Canada."
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



parsing()
CompareDocument()
UploadfilestTos3()   
    
    