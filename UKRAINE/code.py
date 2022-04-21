import hashlib
import json
from datetime import datetime,date,timedelta
import requests
import os
from os.path import exists
import boto3
from copy import deepcopy
import unidecode
from deep_translator import GoogleTranslator
import time
import traceback
import pycountry


#NOTE: Object for output json file
out_list = []
input_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

#NOTE : DAG NAME CHANGE THIS FOR ALL DAGS
dag_name = "ukraine-sanction"

#NOTE: CHECK THE INPUT FILE TYPE (.XLSX, .CSV, .XML, .JSON)
input_filename  = f'{dag_name}-input-{today_date.day}-{today_date.month}-{today_date.year}.json'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'

#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/UKRAINE/"
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"



# -*- Translation Method -*-
def langtranslation(to_translate):
    try:
        translated = GoogleTranslator(source='auto', target='en').translate(to_translate)
    except:
        try:
            translated = GoogleTranslator(source='auto', target='en').translate(to_translate)
        except:
            print(f">>>Translartion Bug : {to_translate}")
            translated = to_translate   
    # translated = to_translate    #NOTE : Only used to skip transaltion.
    return translated

# -*- coding: utf-8 -*-
def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True

# -*- ISO2COUNTRY -*-
iso2Country = {}
for i in pycountry.countries:
    iso2Country[i.alpha_2] = i.name


# -*- UID GENRATOR -*-
def get_hash(n):
    return hashlib.sha256(((n+"National Security and Defence Council of Ukraine Sanctions List, Ukraine").lower()).encode()).hexdigest()    
 

#NOTE: Global Variable for input file from sorce download.
data_indi = []
data_ent = []
def SourceDownload():
    global data_indi,data_ent
    res_indi = requests.get("https://sanctions-t.rnbo.gov.ua/apis/fizosoba/")
    res_ent = requests.get("https://sanctions-t.rnbo.gov.ua/apis/jurosoba/")
    data_indi = json.loads(res_indi.text)
    data_ent = json.loads(res_ent.text)
    data = data_indi+data_ent
    try:
        with open(f'{ip_path}/{input_filename}', "w") as infile:
            infile.write(json.dumps(data,indent=2,ensure_ascii=False))
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename}', "w") as infile:
            infile.write(json.dumps(data,indent=2,ensure_ascii=False))

def process_data():
    global out_list,data_indi,data_ent

    #NOTE: -*- ENTITY -*-
    ent_c = 1
    for i in data_ent:
        uid = str(i["index"]) + "-" + str(i["ukaz_id"])
        names = []
        if i["name_ukr"]:
            names.append(i["name_ukr"])
        if i["name_original"]:
            names.append(i["name_original"])
        if i["name_alternative"]:
            names.append(i["name_alternative"])
        if len(names) > 0:
            name = names[0]
        else:
            name = ""
        # print(f"{ent_c}---->{name}")
        ent_c+=1
        if name:
            if not isEnglish(name):
                name = langtranslation(name)
        else:
            continue #NOTE : there is no name no need to apped into list.
        
        place = i["place"]
        if place:
            if not isEnglish(place):
                place = langtranslation(place)
        else:
            place = ""
        
        adr = [{
            "complete_address" : place,
            "country" : ""
        }]

        if i["odrn_edrpou"]:
            reg_no = unidecode.unidecode(i["odrn_edrpou"])
        else:
            reg_no = ""
        if i["ipn"]:
            tax_no = unidecode.unidecode(i["ipn"])
        else:
            tax_no = ""
        
        docs = {
            "registartion_no" : reg_no,
            "Tax_Identification_Number" : tax_no
        }

        dec_no = i["ukaz_id"]
        if not dec_no:
            dec_no = ""
        dec_date = i["ukaz_date"]
        if not dec_date:
            dec_date = ""
        restr_end_date = i["restriction_end_date"]
        if not restr_end_date:
            restr_end_date = "Unknown"
        sanc_details = {
            "decree_no" : dec_no,
            "date_of_decree" : dec_date
        }

        
        out_list.append({
            "uid": get_hash(uid + name),
            "name": name,
            "alias_name" : names,
            "country" : [],
            "address" : adr,
            "last_updated": last_updated_string,
            "nns_status": "False",
            "list_type" : "Entity",
            "entity_details" : {},
            "sanctions_details" : sanc_details,
            "documents":docs ,
            "list_id" :"UKR_T30090",
            "comment" : "",
            "sanction_list": {
                "sl_authority": "National Security and Defense Council, Ukraine",
                "sl_url": "https://sanctions-t.rnbo.gov.ua/",
                "sl_host_country": "Ukraine",
                "sl_type": "Sanctions",
                "watch_list": "European Watchlists",
                "sl_source": "National Security and Defence Council of Ukraine Sanctions List, Ukraine",
                "sl_description": "Sanctioned imposed by The National Security and Defense Council, Ukraine."
            } 
        })

    #NOTE: -*- INDIVIDUAL -*-
    indi_c = 1
    for i in data_indi:
        uid = str(i["index"]) + "-" + str(i["ukaz_id"])
        names = []
        if i["name_ukr"]:
            names.append(i["name_ukr"])
        if i["name_original"]:
            names.append(i["name_original"])
        if i["name_alternative"]:
            names.append(i["name_alternative"])
        if len(names) > 0:
            name = names[0]
        else:
            name = ""
        # print(f"{indi_c}---->{name}")
        indi_c+=1
        if name:
            if not isEnglish(name):
                name = langtranslation(name)
        else:
            continue #NOTE : there is no name no need to apped into list.
        
        dob = []
        if i["birthdate"]:
            dob.append(i["birthdate"])
        
        country = []
        citizenship = ""
        if i["citizenship"]:
            citizenship = i["citizenship"]
        if citizenship:
            cont = iso2Country.get(citizenship)
            if cont:
                country.append(cont)
    
        place = i["livingplace"]
        if place:
            if not isEnglish(place):
                place = langtranslation(place)
        else:
            place = ""
        adr = [{
            "complete_address" : place,
            "country" : ""
        }]

        Bplace = i["birthplace"]
        if Bplace:
            if not isEnglish(Bplace):
                Bplace = langtranslation(Bplace)
        else:
            Bplace = ""
        Badr = [{
            "complete_address" : Bplace,
            "country" : ""
        }]
        
        idi_det = {
            "date_of_birth" : dob,
            "place_of_birth" : Bplace
        } 

        dec_no = i["ukaz_id"]
        if not dec_no:
            dec_no = ""
        dec_date = i["ukaz_date"]
        if not dec_date:
            dec_date = ""
        restr_end_date = i["restriction_end_date"]
        if not restr_end_date:
            restr_end_date = "Unknown"
        sanc_details = {
            "decree_no" : dec_no,
            "date_of_decree" : dec_date
        }

        
        out_list.append({
            "uid": get_hash(uid+name),
            "name": name,
            "alias_name" : names,
            "country" : country,
            "address" : adr,
            "last_updated": last_updated_string,
            "nns_status": "False",
            "list_type" : "Individual",
            "individual_details" : idi_det,
            "sanctions_details" : sanc_details,
            "documents":{} ,
            "list_id" :"UKR_T30090",
            "comment" : "",
            "sanction_list": {
                "sl_authority": "National Security and Defense Council, Ukraine",
                "sl_url": "https://sanctions-t.rnbo.gov.ua/",
                "sl_host_country": "Ukraine",
                "sl_type": "Sanctions",
                "watch_list": "European Watchlists",
                "sl_source": "National Security and Defence Council of Ukraine Sanctions List, Ukraine",
                "sl_description": "Sanctioned imposed by The National Security and Defense Council, Ukraine."
            } 
        })
    
    total_profile_available = len(out_list)
    print(f"Total profile available: {total_profile_available}")
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
        raise ValueError("Error : Data Parsing Error.... fix it quick ⚒️")

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



SourceDownload()
process_data()
CompareDocument()
UploadfilestTos3()