import hashlib
import json
import pandas as pd
from datetime import datetime,date,timedelta
import requests
import os
import boto3
from os.path import exists
import unidecode
import copy
import traceback

#NOTE: Object for output json file
out_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "hmt-uk"

input_filename  = f'{dag_name}-input-{today_date.day}-{today_date.month}-{today_date.year}.csv'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/HMT-UK/"
# root = ""
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def sourcedownloder():
    url = "https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv"
    response = requests.get(url)
    try:
        with open(f'{ip_path}/{input_filename}', 'wb') as file:
            file.write(response.content)
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename}', 'wb') as file:
            file.write(response.content)

def process_data():
    global out_list,last_updated_string,total_profile_available
    raw = pd.read_csv(f'{ip_path}/{input_filename}',header=1)
    raw.fillna("",inplace=True)

    graw = raw.groupby('Group ID').agg(lambda x: x.tolist())

    out_list = []
    for index, row in graw.iterrows():
        pass_dict = {}
        gid = index
        names = []
        alias = []
        for i in range(len(row["Name 6"])):
            pl = []
            pl.append(row["Name 6"][i])
            pl.append(row["Name 1"][i])
            pl.append(row["Name 2"][i])
            pl.append(row["Name 3"][i])
            pl.append(row["Name 4"][i])
            pl.append(row["Name 5"][i])
            pl = [j for j in pl if j]
            pn = " ".join(pl)
            names.append(pn)
        
        name = names[0]
        alias = alias + names[1:]
        alias = alias + list(set(row["Name Non-Latin Script"]))
        alias = list(set(alias))
        alias = [i for i in alias if i]
        
        ltype = row["Group Type"][0]

        dob = list(set(row["DOB"]))
        dob = [i for i in dob if i]

        pob_adrs = []
        comp_adr = []
        country = []
        for a in range(len(row["Town of Birth"])):
            tob = row["Town of Birth"][a]
            cob = row["Country of Birth"][a]
            if (tob).strip() !="" and (tob).strip() not in comp_adr:
                comp_adr.append((tob).strip())
                if tob:
                    pob_adrs.append(tob)

        country = country + list(set(row["Nationality"]))
        country = country + list(set(row["Country"]))
        country = [ci for ci in country if ci]
        country = list(set(country))

        passports = []
        nids = []
        passports = passports+  list(set(row["Passport Number"]))
        nids = nids + list(set(row["National Identification Number"]))
        passports = [pi for pi in passports if pi]
        nids = [ni for ni in nids if ni]

        desig = list(set(row["Position"]))
        desig = [di for di in desig if di]
        

        address = []
        u_add = []
        for ad in range(len(row["Address 1"])):
            padl = []
            padl.append(row["Address 1"][ad])
            padl.append(row["Address 2"][ad])
            padl.append(row["Address 3"][ad])
            padl.append(row["Address 4"][ad])
            padl.append(row["Address 5"][ad])
            padl.append(row["Address 6"][ad])
            padl.append(row["Post/Zip Code"][ad])
            padl = [j for j in padl if j]
            pad = ",".join(padl)

            if pad not in u_add:
                u_add.append(pad)
                address.append({
                    "complete_address" : pad,
                    "country" : row["Country"][ad]
                })

        comment = [ri for ri in row["Regime"] if ri][0]


        pass_dict["uid"] = gid
        pass_dict["name"] = name
        pass_dict["alias_name"] = alias
        pass_dict["list_type"] = ltype
        pass_dict["country"] = country
        pass_dict["address"] = address
        if ltype == "Individual":
            pass_dict["individual_details"] = {}
            pass_dict["individual_details"]["date_of_birth"] = dob
            pass_dict["individual_details"]["town_of_birth"] = pob_adrs
            pass_dict["designation"] = desig
        else:
            pass_dict["entity_details"] = {}
        pass_dict["documents"] = {}
        pass_dict["documents"]["passport"] = passports
        pass_dict["documents"]["other_ids"] = nids
        pass_dict["comment"] = comment
        pass_dict["sanctions_list"] = {
            "sl_authority": "The Office of Financial Sanctions Implementation (OFSI) of the HM Treasury, The United Kingdom Government",
            "sl_url": "https://www.gov.uk/government/publications/financial-sanctions-consolidated-list-of-targets/consolidated-list-of-targets",
            "sl_host_country": "United Kingdom",
            "sl_type": "Sanctions",
            "watch_list": "European Watchlists",
            "sl_source": "HM Treasury List",
            "sl_description": "HMT sanctions list is a list of entities and individuals subjected to certain financial restrictions as part of the United Kingdom's government's domestic counter-terrorism regime policy. Also, it includes individuals prohibited by the European Union and/or the United Nations."
        }

        out_list.append(pass_dict)

    total_profile_available = len(out_list)
    print(f"Total profile available: {total_profile_available}")
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=2)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=2)


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



sourcedownloder()
process_data()
CompareDocument()
UploadfilestTos3()