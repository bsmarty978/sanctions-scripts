# -*- coding: utf-8 -*-


import requests
from scrapy.http import HtmlResponse
from datetime import datetime,date,timedelta
import os
from os.path import exists
import json
from copy import deepcopy
import boto3
import hashlib
import camelot


#NOTE: Filename according to the date :
out_list = []
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "tata-851"
root = "/home/ubuntu/sanctions-scripts/TATA-851/"

input_filename  = f'{dag_name}-inout.pdf'
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
    return hashlib.sha256(((n+"Tata AIA Life Insurance Co. Ltd. Black Listed Agents List, India" + "IND_E20360").lower()).encode()).hexdigest()
  
def alias_maker(n):
    na = n.split(" ")
    alis = []
    if len(na)==2:
        alis.append((na[1].strip()+" "+na[0].strip()).strip())
    elif len(na) == 3:
        alis.append((na[2].strip()+" "+na[0].strip() + " "+na[1].strip()).strip())
    return alis

def sourcedownloader():
    header = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
        }

    r =  requests.get("https://www.tataaia.com/know-your-advisor.html",headers=header)

    resp = HtmlResponse("example.com",body=r.text,encoding='utf-8')
    url = "https://www.tataaia.com" + resp.xpath("(//p[@class='help-note'])[6]/a/@href").get()

    res = requests.get(url,headers=header)

    try:
        with open(f'{ip_path}/{input_filename}', "wb") as infile:
            infile.write(res.content)
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename}', "wb") as infile:
            infile.write(res.content)


def process_data():
    global out_list,last_updated_string,total_profile_available

    tb = camelot.read_pdf(f'{ip_path}/{input_filename}', pages="all", flavor="stream")
    print(len(tb))
    # print(tb[0].df.head())
    # print(tb[0].df.columns)
    # exit()
    for d in tb:
        for i,r in d.df.iterrows():
            data_dict = {}
            name = r[0].strip()
            body = name.split(' ')
            
            agent_code = body.pop(0)
            body = ' '.join(body)
            if body == "" or body == "Code\nName of the Agent":
                continue
            dob = [r[2].strip()] if r[2].strip() else []
            pan = [r[1].strip()] if r[1].strip() else []
            termination = r[3].strip()
            termination = termination.split(' ')
            issue_date = termination.pop(0)
            termination =' '.join(termination)
            # print(body, pan, termination,dob,issue_date)
            data_dict['uid'] = get_hash(body)
            data_dict['name'] = body
            data_dict['alias_name'] = alias_maker(body)
            data_dict['list_type'] = 'Individual'
            data_dict['last_updated'] = last_updated_string
            data_dict['nns_status'] = 'False'
            data_dict['sanction_details'] = {'issue_date':issue_date, 'reason':termination}
            data_dict['individual_details'] = {"date_of_birth":dob,"gender":""}
            data_dict['country']=['India']
            data_dict['address'] = [{'complete_addrss':'','country':''}]
            data_dict['comment'] = ''
            data_dict['documents'] = {'agent_code':agent_code, 'PAN':pan}
            data_dict["sanction_list"]= {
                "sl_authority": "Tata AIA Life Insurance Co. Ltd., India",
                "sl_url": "https://www.tataaia.com/know-your-advisor.html",
                "sl_host_country": "India",
                "sl_type": "Sanctions",
                "watch_list": "India Watchlists",
                "sl_source": "Tata AIA Life Insurance Co. Ltd. Black Listed Agents List, India",
                "sl_description": "List of the Black listed agents as per IRDAI guidelines issued by Tata AIA Life Insurance Co. Ltd., India",
                "list_id": "IND_E20360"
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
            passing = f"{last_updated_string},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(passing)
    else:
        with open(f'{lp_path}',"a") as outfile:
            pass_first = "date,inputfile,outputfile,total_profile_availabe,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
            passing = f"{last_updated_string},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(pass_first)
            outfile.write(passing)

def UploadfilestTos3():
    try:
        print("uploading files to s3")
        s3 = boto3.client('s3')
        s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"{dag_name}/original/{input_filename}")
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

        

