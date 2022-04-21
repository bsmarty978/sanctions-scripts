from re import I
import requests
import json
import pycountry
from deep_translator import GoogleTranslator
import os
from datetime import datetime as dt
from datetime import timedelta,date
import time
import traceback
import copy
import boto3
import hashlib
from os.path import exists



#NOTE: Object for output json file
out_obj = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = dt.now().strftime("%Y-%m-%dT%H:%M:%S")

input_filename  = f'jp-sr-{today_date.day}-{today_date.month}-{today_date.year}.json'
output_filename = f'jp-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'diffrance-jp-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'removed-jp-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'jp-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'

#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/JAPAN/"
# root = ""
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}japan-logfile.csv"


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


def fileprocessor():
    print(f"------File Downloading started--------------------")
    #NOTE : DATA DOWNLOADING STARTS FROM HERE
    URL = "https://data.opensanctions.org/datasets/latest/jp_mof_sanctions/targets.nested.json"
    response = requests.get(URL)
    dum = response.text
    duml = []
    for i in dum.split("\n"):
        try:
            if i:
                duml.append(json.loads(i))
        except:
            print(f">>>>>>>{i}")
            break
    
    try:
        with open(f'{ip_path}/{input_filename}', 'w', encoding='utf-8') as file:
            json.dump(duml, file, ensure_ascii=False)
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename}', 'w', encoding='utf-8') as file:
            json.dump(duml, file, ensure_ascii=False)
    print(f"------File Downloading completed--------------------")

    print(f"------File Proccesing started----------------------")
    #NOTE: DATA PROCESSING STARTS FROM HERE
    for prof in duml:
        base_obj = {}
        uid = hashlib.sha256(((prof["id"] +"SANCTIONS JAPAN list").lower()).encode()).hexdigest()
        base_obj["uid"] = uid
        base_obj["name"] = prof["caption"]

        base_obj["alias_name"] = []
        base_obj["alias_name"] = prof["properties"].get("alias")
        for i in prof["properties"]["name"]:
            if i!= prof["caption"]:
                base_obj["alias_name"].append(i)

        if prof["properties"].get("weakAlias"):
            for j in prof["properties"]["weakAlias"]:
                al = j.strip().strip(")").strip()
                if al not in base_obj["alias_name"]:
                    base_obj["alias_name"].append(al)

        base_obj["list_type"] = ""
        if prof["schema"] == "Person":
            base_obj["list_type"] = "Individual"
        elif prof["schema"] == "LegalEntity":
            base_obj["list_type"] = "Entity"
        elif prof["schema"] == "Organization":
            base_obj["list_type"] = "Entity"
        else:
            base_obj["list_type"] = "Unknown"

        base_obj["country"] = []
        base_obj["address"] = []
        if prof["properties"].get("addressEntity"):
            for adr in prof["properties"]["addressEntity"]:
                try:
                    comp_addr = ""
                    country = ""
                    comp_addr = adr["caption"].split("(")[0].strip()
                    if len(comp_addr.split(",")) > 1:
                            country = comp_addr.split(",")[-1].strip().strip(";").strip()
                    if adr.get("properties").get("country"):
                        try:
                            country = pycountry.countries.get(alpha_2=adr["properties"]["country"][0]).name
                        except:
                            country = ""
                    if not isEnglish(comp_addr):
                        comp_addr = langtranslation(comp_addr)
                    base_obj["address"].append({
                        "complete_address" : comp_addr,
                        "country" : country
                    })
                    if country != "" and country not in base_obj["country"]:
                        base_obj["country"].append(country)
                except Exception as e:
                    print(e)
                    print(prof["caption"])

        base_obj["comment"] = ""
        if prof["properties"].get("notes"):
            base_obj["comment"] = langtranslation(prof["properties"]["notes"][0])

        designation = []
        if prof["properties"].get("position"):
            for desi in prof["properties"]["position"]:
                if not isEnglish(desi):
                    designation.append(langtranslation(desi))
                else: 
                    designation.append(desi)
                
        
        if prof["properties"].get("country"):
            for con in prof["properties"]["country"]:
                try:
                    c_val= pycountry.countries.get(alpha_2=con).name
                except:
                    c_val= ""
                if c_val not in base_obj["country"] and c_val!="":
                    base_obj["country"].append(c_val)
        
        dobs = []
        if prof["properties"].get("birthDate"):
            for d in prof["properties"]["birthDate"]:
                dobs.append(d)

        pobs = []
        if prof["properties"].get("birthPlace"):
            for p in prof["properties"]["birthPlace"]:
                if not isEnglish(p):
                    pobs.append(langtranslation(p))
                else:
                    pobs.append(p)


        possnos = []
        if prof["properties"].get("passportNumber"):
            for pa in prof["properties"]["passportNumber"]:
                if not isEnglish(pa):
                    possnos.append(langtranslation(pa))
                else:
                    possnos.append(pa)

        # titles = []
        # if prof["properties"].get("title"):
        #     for ti in prof["properties"]["title"]:
        #         tite = ti.split("(")
        #         if len(tite) == 2:
        #             if isEnglish(tite[0].strip()):
        #                 titles.append(tite[0].strip())
        #             elif isEnglish(tite[1].strip()):
        #                 titles.append(tite[1].strip())
        #             else:
        #                 titles.append(langtranslation(ti))
        
        phonesnos = []
        if prof["properties"].get("phone"):
            phonesnos =  prof["properties"]["phone"]

        # docs = []
        # if prof["properties"].get("idNumber"):
        #     for do in prof["properties"]["idNumber"]:
        #         if not isEnglish(do):
        #             docs.append(langtranslation(do))
        #         else:
        #             docs.append(do)

        if base_obj["list_type"]=="Individual":
            base_obj["individual_details"] = {
                "date_of_birth" : dobs,
                "place_of_birth" : pobs,
                "designation" : designation,
                "phone" :phonesnos
            }
        else:
            base_obj["entity_details"] = {
                "phone" :phonesnos
            }

            base_obj["documets"] = {
                "passport" : possnos
            }

        authority = ""
        program = ""
        if prof["properties"]["sanctions"][0]["properties"].get("authority"):
            authority =prof["properties"]["sanctions"][0]["properties"]["authority"]
        
        # if prof["properties"]["sanctions"][0]["properties"].get("program"):
        #     program =prof["properties"]["sanctions"][0]["properties"]["program"]
        base_obj["sanctions_details"] = {
            "authority" : authority[0],
        }
        base_obj["last_updated"] = last_updated_string

        base_obj["list_id"] =  "JPN_S10012"
        base_obj["sanction_list"] = {
            "sl_authority": "Minister of Finance, Japan",
            "sl_url": "https://www.opensanctions.org/datasets/jp_mof_sanctions/",
            "sl_host_country": "Japan",
            "watch_list" : "APAC Watchlists",
            "sl_type": "Sanctions",
            "sl_source": "Consolidated Japan Sanctions List",
            "sl_description": "The list of designated people and organizations by Minister of Finance, Japan."
            }

        out_obj.append(base_obj)


    print(f"------File Proccesing completed----------------------")
    global total_profile_available
    total_profile_available = len(out_obj)
    #NOTE:Saving outputfile localy 
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_obj, outfile,ensure_ascii=False)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_obj, outfile,ensure_ascii=False)

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
    new_list = copy.deepcopy(out_obj)

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
    #NOTE: ---------- META LOg FIlE --------------------------
    try:
        with open('/home/ubuntu/meta-scrap-log.json', 'rb') as f:
            logd = f.read()
            pred = json.loads(logd)
            pred.append({
                "dag" : "JAPAN",
                "date" : last_updated_string,
                "total" : len(new_list),
                "new" :  len(new_profiles),
                "update" : len(updated_profiles),
                "remove" : len(removed_profiles),
            })
        with open('/home/ubuntu/meta-scrap-log.json', 'w') as f:
            json.dump(pred,f)
    except:
        print("ERROR WHILE UPDATING META LOG FILE")
    #NOTE:  -----------------------------------------------------


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
            passing = f"{today_date.ctime()},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(passing)
    else:
        with open(f'{lp_path}',"a") as outfile:
            pass_first = "date,inputfile,outputfile,total_profile_availabe,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
            passing = f"{last_updated_string},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(pass_first)
            outfile.write(passing)

#NOTE : Method to upload files in s3 bucket (Bhavesh Chavda)
def UploadfilestTos3():
    try:
        print("uploading files to s3")
        s3 = boto3.client('s3')
        s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"japan/original/{input_filename}")
        s3.upload_file(f'{op_path}/{output_filename}',"sams-scrapping-data",f"japan/parced/{output_filename}")
        s3.upload_file(f'{dp_path}/{diffrance_filename}',"sams-scrapping-data",f"japan/diffrance/{diffrance_filename}")
        s3.upload_file(f'{rm_path}/{removed_filename}',"sams-scrapping-data",f"japan/removed/{removed_filename}")
        s3.upload_file(f'{lp_path}',"sams-scrapping-data","japan/japan-logfile.csv")
        print("uploaded files to s3")      
    except Exception as e:
        print("------------------🔴ALERT🔴------------------------")
        print("Can not upload files to s3")
        print("Exception : " , e)
        print("----------------------------------------------------")



start_time = time.time()
try:
    fileprocessor()
    CompareDocument()
    UploadfilestTos3()
    print(f"Total Profile Available:{total_profile_available}")
    print(f"--- {time.time() - start_time} seconds ---")
except Exception as e:
    print("------------------🔴ALERT🔴------------------------")
    print("Exception : " , e)
    print("Exception : " , traceback.print_exc())
    print(f"--- {time.time() - start_time} seconds ---")
    print("----------------------------------------------------")