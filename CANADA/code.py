import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime as dt
from datetime import timedelta,date
import copy
import hashlib
import time
import traceback
import os
from os.path import exists
import unidecode
from deep_translator import GoogleTranslator
import boto3

#NOTE: Object for output json file
out_obj = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = dt.now().strftime("%Y-%m-%dT%H:%M:%S")

input_filename  = f'canada-xml-{today_date.day}-{today_date.month}-{today_date.year}.xml'
output_filename = f'canada-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'diffrance-canada-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'removed-canada-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'canada-json-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'

#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/CANADA/"
# root = ""
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}canda-logfile.csv"


def xmlfiledownloder():
    URL = "https://www.international.gc.ca/world-monde/assets/office_docs/international_relations-relations_internationales/sanctions/sema-lmes.xml"
    response = requests.get(URL)
    try:
        with open(f'{ip_path}/{input_filename}', 'wb') as file:
            file.write(response.content)
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename}', 'wb') as file:
            file.write(response.content)
    

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


def alifinder(an):
    reli = []
    if an.strip().endswith(")"):
        br = an.strip().split("(")
        for i in br:
            reli.append(i.strip().strip(")").strip())
    else:
        reli.append(an.strip())
    return reli


def fileprocessor():
    with open(f'{ip_path}/{input_filename}', 'rb') as f:
        data = f.read()
    # with open(f'consolidated-list_2022-01-28.xml', 'rb') as f:
    #     data = f.read()
    
    Bs_data = BeautifulSoup(data, "xml")
    entries = Bs_data.find_all("record")

    for entry in entries:
        pass_obj = {}    
        name = ""
        nal = []
        if entry.find("Entity"):
            name = name + " " + entry.find("Entity").text
            if len(name.split("/"))>1:
                nc = 0
                nal = []
                for j in name.split("/"):
                    if nc==0:
                        name = j.strip()
                        nc+=1
                    else:
                        nal.append(j.strip().strip(")").strip("(").strip())
                # break


        name = name.strip()

        if name == "":
            if entry.find("GivenName"):
                name = name + " " + entry.find("GivenName").text
            name = name.strip()

            if entry.find("LastName"):
                name = name + " " + entry.find("LastName").text
            name = name.strip()

            list_type = "Individual"
        else:
            list_type = "Entity"

        
        country = []
        if entry.find("Country"):
            constr = entry.find("Country").text
            if len(constr.split("/")) > 1:
                country.append(constr.split("/")[0].strip())
            else:
                country.append(constr.strip())    
        
        dobs = []
        if entry.find("DateOfBirth"):
            dobs.append(entry.find("DateOfBirth").text)
        
        Aliases = []
        if entry.find("Aliases"):
            alis = entry.find("Aliases").text
            alislist = alis.split(",")
            newa = []
            if len(alislist) > 1:
                for al in alislist:
                    for n in alifinder(al):
                        newa.append(n)
                Aliases = newa
            else:
                Aliases = alislist
        for l in nal:
            Aliases.append(l)

        if Aliases == []:
            namespli = name.split(" ")
            if len(namespli) > 1 and len(namespli) < 3:
                Aliases.append(namespli[1].strip() + " " + namespli[0].strip())
                Aliases.append(namespli[0].strip() + " " + namespli[1].strip())
            elif len(namespli) >= 3:
                Aliases.append(namespli[0].strip() + " " + namespli[1].strip())
                Aliases.append(namespli[2].strip() + " " + namespli[1].strip())
                Aliases.append(namespli[0].strip() + " " + namespli[2].strip())
                Aliases.append(namespli[1].strip() + " " + namespli[2].strip())
            else:
                Aliases.append(name)


        country.sort()
        Aliases = list(set(Aliases))
        Aliases.sort()

        pass_obj["uid"] = hashlib.sha256(((name + country[0] + list_type + Aliases[0] +"CANADA SANCTIon LIST").lower()).encode()).hexdigest()
        pass_obj["name"] = name
        pass_obj["list_type"] = list_type
        pass_obj["country"] = country
        pass_obj["Aliases"] = Aliases
        pass_obj["last_updated"] = last_updated_string
        pass_obj["address"] = [{"complete_address":"","country":""}]
        pass_obj["documents"] = {}
        pass_obj["nns_status"] = False
        if list_type == "Individual":
            pass_obj["individual_details"] = {
                "date_of_birth" : dobs,
                "place_of_birth" : [],
            }
        else:
            pass_obj["entity_details"] =  {}

        pass_obj["sanction_Details"] = {}
        pass_obj["comment"] = ""
        pass_obj["list_id"] = "CAN_S10003"
        pass_obj["sanction_list"] = {
            "sl_authority": "Global Affairs Canada, Government of Canada",
            "sl_url": "https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/consolidated-consolide.aspx?lang=eng",
            "sl_host_country": "Canada",
            "sl_type": "Sanctions",
            "watch_list" : "North America Watchlists",
            "sl_source": "Global Affairs Canada, Consolidated Canadian Autonomous Sanctions List",
            "sl_description": "The Consolidated Canadian Autonomous Sanctions List includes individuals and entities subject to specific sanctions regulations made under the Special Economic Measures Act (SEMA) and the Justice for Victims of Corrupt Foreign Officials Act (JVCFOA)."
            }
        
        out_obj.append(pass_obj)

    global total_profile_available
    total_profile_available = len(entries)

    #NOTE:Saving outputfile localy 
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_obj, outfile,ensure_ascii=False)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_obj, outfile,ensure_ascii=False)
    
    if out_obj==[]:
        raise ValueError("Error : Data Parsing Error.... fix it quickly ⚒️")



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
    if len(new_list)==0:
        removed_profiles = []
        raise ValueError("Data Parsing Error Fix it Quickly ⚒️")
    print("------------------------LOG-DATA---------------------------")
    print(f"total New Profiles Detected:     {len(new_profiles)}")
    print(f"total Updated Profiles Detected: {len(updated_profiles)}")
    print(f"total Removed Profiles Detected: {len(removed_profiles)}")
    print("-----------------------------------------------------------")

    #NOTE: ---------- META LOg FIlE --------------------------
    try:
        with open('/home/ubuntu/meta-scrap-log.json', 'rb') as f:
            logd = f.read()
            pred = json.loads(logd)
            pred.append({
                "dag" : "CANADA",
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
            passing = f"{last_updated_string},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
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
        s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-automation",f"canada/original/{input_filename}")
        s3.upload_file(f'{op_path}/{output_filename}',"sams-scrapping-automation",f"canada/parced/{output_filename}")
        s3.upload_file(f'{dp_path}/{diffrance_filename}',"sams-scrapping-automation",f"canada/diffrance/{diffrance_filename}")
        s3.upload_file(f'{rm_path}/{removed_filename}',"sams-scrapping-automation",f"canada/removed/{removed_filename}")
        s3.upload_file(f'{lp_path}',"sams-scrapping-automation","canada/canada-logfile.csv")
        print("uploaded files to s3")      
    except Exception as e:
        print("------------------🔴ALERT🔴------------------------")
        print("Can not upload files to s3")
        print("Exception : " , e)
        print("----------------------------------------------------")


start_time = time.time()
try:
    print(f"------File Downloading started--------------------")
    xmlfiledownloder()
    print(f"------File Downloading Completed------------------")
    print(f"------File Proccesing started----------------------")
    fileprocessor()
    print(f"------File Proccesing Completed----------------------")
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