from ast import Pass
import encodings
import pandas as pd
import json
from unidecode import unidecode
import os
from os.path import exists
import boto3
from copy import deepcopy
from datetime import datetime,date,timedelta
import hashlib
import requests
from deep_translator import GoogleTranslator
from scrapy.http import HtmlResponse

#NOTE: Object for output json file
out_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "MONACO"

input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.xlsx'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/MONACO/"
# root = ""
ip_path = f"{root}inputfiles"
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
    return hashlib.sha256(((n+"Monaco National Asset Freeze Economic Resources Sanctions List, Monaco" + "MCO_S10050").lower()).encode()).hexdigest()

def sourcedownloader():
    r = requests.get("https://en.service-public-entreprises.gouv.mc/Conducting-business/Legal-and-accounting-obligations/Asset-freezing-measures/National-asset-freezing-and-economic-resources-list")
    resp = HtmlResponse("example.com",body=r.text,encoding="utf-8")
    ur = resp.xpath("//a[@title='Download file Liste nationale']/@href").get()
    url = "https://en.service-public-entreprises.gouv.mc"+ur
    r = requests.get(url)
    try:
        with open(f'{ip_path}/{input_filename}', 'wb') as file:
            file.write(r.content)
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename}', 'wb') as file:
            file.write(r.content)


def process_data():
    global out_list,last_updated_string,total_profile_available
    df = pd.read_excel(f'{ip_path}/{input_filename}',header=1)
    df.fillna("",inplace=True)

    for index,row in df.astype(str).iterrows():
        pass_dict = {}

        name  = row["Nom"]
        first_name = row["Prénom"]
        Name = (first_name.strip() + " " + name.strip()).replace("”","").replace("“","").replace('"',"").strip()
        Name = unidecode(Name)
        pass_dict["uid"] = get_hash(Name)
        Name = Name.replace("_x000D_\n","")
        names = Name.split(",")
        pass_dict["name"] = names[0]
        print(Name)
        alias = []
        alias = row["Alias"].replace("_x000D_\n","")
        if alias:
            alias = alias.split(",")
        else:
            alias = []
        alias = alias + names
        alias =  [k for k in alias if k.strip()]
        pass_dict["alias_name"] = list(set(alias))

        pass_dict["last_updated"] = last_updated_string
        pass_dict["nns_status"] = "False"
        pass_dict["list_id"] = "MCO_S10050"

        regime = row['Régime de sanctions '].replace("_x000D_\n","")
        pass_dict["sanction_details"] = {}
        pass_dict["sanction_details"]["sanctions_body"] = regime
        
        adrs = []
        country = []
        pobs = row["Lieu de Naissance"].replace("_x000D_\n","")
        if pobs:
            pobs = pobs.split(",")
        pobs =  [k for k in pobs if k.strip()]
        for j in pobs:
            if ":" in j:
                con = j.split(":")[-1].strip()
                comp = j.replace(":",",")
                adrs.append({
                    "complete_address" : langtranslation(comp),
                    "country" : con
                })
                country.append(con)
            else:
                adrs.append({
                    "complete_address" : langtranslation(j),
                    "country" : ""
                })
        country = list(set(country))
        pass_dict["country"] = country
        pass_dict["address"] = adrs

        ltype = row["Nature"]
        if ltype == "Personne physique":
            ltype= "Individual"
            dobs = row["Date de Naissance"].replace("_x000D_\n","")
            if dobs:
                dobs = dobs.split(",")
            dobs =  [k for k in dobs if k.strip()]
            pass_dict["list_type"] = "Individual"
            pass_dict["individual_details"] = {}
            pass_dict["individual_details"]["date_of_birth"] = dobs 

            
            gender = row["Sexe"]
            if gender == "Féminin":
                gender = "female"
            elif gender == "Masculin":
                gender = "male"
            pass_dict["individual_details"]["gender"] = gender 

            nationalty = row["Nationalité"].replace("_x000D_\n","")
            pass_dict["individual_details"]["nationality"] = nationalty
            
        elif ltype == "Personne morale":
            ltype = "Entity"
            pass_dict["list_type"] = "Entity"
            pass_dict["entity_details"] = {}

        elif ltype == "Navire":
            ltype = "Vessel"
            pass_dict["list_type"] = "Vessel"
            pass_dict["vessel_details"] = {}
        else:
            ltype = "Entity"
            pass_dict["list_type"] = "Entity"
            pass_dict["entity_details"] = {}
        
        pass_dict["documents"] = {}
        pass_dict["comment"] = ""

        pass_dict["sanction_list"] = {
            "sl_authority": "Department of Budget and Treasury, Monaco",
            "sl_url": "https://en.service-public-entreprises.gouv.mc/Conducting-business/Legal-and-accounting-obligations/Asset-freezing-measures/National-asset-freezing-and-economic-resources-list",
            "watch_list": "European Watchlists",
            "sl_host_country": "Monaco",
            "sl_type": "Sanctions",
            "sl_source": "Monaco National Asset Freeze Economic Resources Sanctions List, Monaco",
            "sl_description": "List of individuals and entities sanctioned by Department of Budget and Treasury, Monaco"
        }

        out_list.append(pass_dict)
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
