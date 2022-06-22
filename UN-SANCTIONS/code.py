from bs4 import BeautifulSoup
import json
from datetime import datetime as dt
from datetime import timedelta,date
import hashlib
import requests
import copy
from deep_translator import GoogleTranslator
import boto3
import time
import traceback
import os
from os.path import exists

#NOTE: Object for output json file
out_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = dt.now().strftime("%Y-%m-%dT%H:%M:%S")

input_filename  = f'un-xml-{today_date.day}-{today_date.month}-{today_date.year}.xml'
output_filename = f'un-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'diffrance-un-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'removed-un-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'un-json-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'

#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/UN-SANCTIONS/"
# root = ""
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}un-sanctions-logfile.csv"


def alin_gen(nmstr):
    out = []
    nm = nmstr.split(' ')
    if len(nm)==2:
        out.append(nm[1].strip() + " " + nm[0].strip())
    if len(nm) > 2:
        out.append(nm[0].strip() + " " + nm[2].strip())
        out.append(nm[2].strip() + " " + nm[0].strip())
        out.append(nm[1].strip() + " " + nm[2].strip())
        out.append(nm[2].strip() + " " + nm[1].strip())
    else:
        out.append(nmstr)
    return out


def xmlfiledownloder():
    URL = "https://scsanctions.un.org/resources/xml/en/consolidated.xml"
    response = requests.get(URL)
    try:
        with open(f'{ip_path}/{input_filename}', 'wb') as file:
            file.write(response.content)
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename}', 'wb') as file:
            file.write(response.content)

def fileprocessor():
    with open(f'{ip_path}/{input_filename}', 'rb') as f:
        data = f.read()
    # with open(f'consolidated-list_2022-01-28.xml', 'rb') as f:
    #     data = f.read()

    Bs_data = BeautifulSoup(data, "xml")

    global total_profile_available
    entries = Bs_data.find_all("INDIVIDUAL")
    total_profile_available += len(entries)

    global out_list
    for entry in entries:
        out_obj = {}
        dataid = entry.find("DATAID").text
        name = ""
        first_name = entry.find("FIRST_NAME").text
        name = name.strip() + " " + first_name

        if entry.find("SECOND_NAME"):
            sec_name = entry.find("SECOND_NAME").text
            name = name.strip() + " " + sec_name
        
        if entry.find("THIRD_NAME"):
            third_name = entry.find("THIRD_NAME").text
            name = name.strip() + " " + third_name
        
        if entry.find("FOURTH_NAME"):
            fourth_name = entry.find("FOURTH_NAME").text
            name = name.strip() + " " + fourth_name
        

        name = name.strip()
        lstype = "Individual"

        unlstype = entry.find("UN_LIST_TYPE").text
        refno = entry.find("REFERENCE_NUMBER").text
        listname = entry.find("LISTED_ON").text

        gender = ""
        if entry.find("GENDER"):
            gender = entry.find("GENDER").text
        
        oriname = []
        if entry.find_all("NAME_ORIGINAL_SCRIPT"):
            for orn in entry.find_all("NAME_ORIGINAL_SCRIPT"):
                oriname.append(orn.text)

        comment = ""
        if entry.find("COMMENTS1"):
            comment = entry.find("COMMENTS1").text.strip()

        design = []
        if entry.find("DESIGNATION"):
            for desi in entry.find("DESIGNATION").find_all("VALUE"):
                design.append(desi.text)

        nationality = []
        if entry.find("NATIONALITY"):
            for nesi in entry.find("NATIONALITY").find_all("VALUE"):
                nationality.append(nesi.text)
        
        alias = []
        for ori in oriname:
            alias.append(ori)
        dobs = []
        pobs = []

        for alimeta in entry.find_all("INDIVIDUAL_ALIAS"):
            if alimeta.find("ALIAS_NAME"):
                if alimeta.find("ALIAS_NAME").text:
                    alias.append(alimeta.find("ALIAS_NAME").text.strip())

            if alimeta.find("DATE_OF_BIRTH"):
                if alimeta.find("DATE_OF_BIRTH").text:
                    dobs.append(alimeta.find("DATE_OF_BIRTH").text.strip())

            adr = {"complete_address" :"" , "country" : ""}
            if alimeta.find("CITY_OF_BIRTH"):
                if alimeta.find("CITY_OF_BIRTH").text:
                    adr["complete_address"] = alimeta.find("CITY_OF_BIRTH").text.strip()

            if alimeta.find("COUNTRY_OF_BIRTH"):
                if alimeta.find("COUNTRY_OF_BIRTH").text:
                    adr["country"] = alimeta.find("COUNTRY_OF_BIRTH").text.strip()
                    nationality.append(adr["country"])

            # if adr["complete_address"] or adr["country"]:
            #     pobs.append(adr)


        address = []
        for adrs in entry.find_all("INDIVIDUAL_ADDRESS"):
            adr_ob = {"completed_address": "", "zip": "", "country": ""}
            comp_adr = ""

            if adrs.find("STREET"):
                if adrs.find("STREET").text:
                    comp_adr = comp_adr + "," + adrs.find("STREET").text
            if adrs.find("CITY"):
                if adrs.find("CITY").text:
                    comp_adr = comp_adr + "," + adrs.find("CITY").text
            if adrs.find("STATE_PROVINCE"):
                if adrs.find("STATE_PROVINCE").text:
                    comp_adr = comp_adr + "," + adrs.find("STATE_PROVINCE").text
            if adrs.find("ZIP_CODE"):
                if adrs.find("ZIP_CODE").text:
                    comp_adr = comp_adr + "," + adrs.find("ZIP_CODE").text
                    adr_ob["zip"] = adrs.find("ZIP_CODE").text
            if adrs.find("COUNTRY"):
                if adrs.find("COUNTRY").text:
                    comp_adr = comp_adr + "," + adrs.find("COUNTRY").text
                    adr_ob["country"] = adrs.find("COUNTRY").text

            comp_adr = comp_adr.strip().strip(",").strip()
            adr_ob["completed_address"] = comp_adr

            if adr_ob["completed_address"] or adr_ob["zip"] or adr_ob["country"]:
                address.append(adr_ob)

        for indob in entry.find_all("INDIVIDUAL_DATE_OF_BIRTH"):
            if indob.find("DATE"):
                if indob.find("DATE").text:
                    dobs.append(indob.find("DATE").text.strip())
            if indob.find("YEAR"):
                if indob.find("YEAR").text:
                    dobs.append(indob.find("YEAR").text.strip())
            if indob.find("FROM_YEAR"):
                if indob.find("FROM_YEAR").text:
                    dobs.append(indob.find("FROM_YEAR").text.strip())
            if indob.find("TO_YEAR"):
                if indob.find("TO_YEAR").text:
                    dobs.append(indob.find("TO_YEAR").text.strip())

        for inplace in entry.find_all("INDIVIDUAL_PLACE_OF_BIRTH"):
            co_adr = ""
            cont = ""
            if inplace.find("CITY"):
                if inplace.find("CITY").text:
                    co_adr = co_adr + "," + inplace.find("CITY").text
            if inplace.find("STATE_PROVINCE"):
                if inplace.find("STATE_PROVINCE").text:
                    co_adr = co_adr + "," + inplace.find("STATE_PROVINCE").text
            if inplace.find("COUNTRY"):
                if inplace.find("COUNTRY").text:
                    co_adr = co_adr + "," + inplace.find("COUNTRY").text
                    cont = inplace.find("COUNTRY").text.strip()
            co_adr = co_adr.strip().strip(",").strip()

            if co_adr or cont:
                pobs.append(co_adr)

        docs = {}
        for doc in entry.find_all("INDIVIDUAL_DOCUMENT"):
            if doc.find("TYPE_OF_DOCUMENT"):
                if doc.find("TYPE_OF_DOCUMENT").text:
                    dname = doc.find("TYPE_OF_DOCUMENT").text.strip()
                    # print(dname)
                    if dname == "National Identification Number":
                        dname = "id"
                    elif dname == "Passport":
                        dname = "passport"
                    else:
                        dname = "otherid"

                doc_num = ""
                if doc.find("NUMBER"):   
                    if doc.find("NUMBER").text:
                        doc_num =  doc.find("NUMBER").text.strip()
                        # print(doc_num)

                if doc_num:
                    try:
                        docs[dname].append(doc_num)
                    except KeyError:
                        docs[dname] = []   
                        docs[dname].append(doc_num)

        if address == []:
            address.append({"completed_address":"","country" : ""})
        if alias == []:
           alias = alin_gen(name)
        
        out_obj["uid"] =  hashlib.sha256(((dataid +"UN SANCTIONS").lower()).encode()).hexdigest()
        out_obj["name"] = name
        out_obj["list_type"] = lstype
        out_obj["sanctions_details"] = {
            "un_list_type": unlstype,
            "refrence_no"   : refno,
            # "listed_on" : listname
        }
        out_obj["country"] = nationality
        out_obj["designation"] = design
        out_obj["address"] = address
        out_obj["alias"] = alias
        out_obj["individual_details"] = {
            "gender" : gender,
            "place_of_birth":pobs,
            "date_of_birth" :dobs
        }
        out_obj["nns_status"] = False
        out_obj["last_updated"] = last_updated_string
        out_obj["documents"] = docs
        out_obj["comment"] = comment.strip()
        # out_obj["list_id"] = "UN_S10005"
        out_obj["sanction_list"] = {
                "sl_authority": "United Nations Security Council",
                "sl_url": "https://www.un.org/securitycouncil/content/un-sc-consolidated-list",
                "watch_list": "Global Watchlists",
                "sl_host_country": "International",
                "sl_type": "Sanctions",
                "list_id" : "UN_S10005",
                "sl_source": "United Nations Security Council Consolidated Sanctions List, United Nations",
                "sl_description" : "The Consolidated List includes all individuals and entities subject to measures imposed by the Security Council."
        }

        out_list.append(out_obj)
    
    entries = Bs_data.find_all("ENTITY")
    total_profile_available += len(entries)

    for entry in entries:
        out_obj = {}
        dataid = entry.find("DATAID").text
        first_name = entry.find("FIRST_NAME").text
        name = first_name.strip()
        lstype = "Entity"
        
        unlstype = entry.find("UN_LIST_TYPE").text
        refno = entry.find("REFERENCE_NUMBER").text
        listname = entry.find("LISTED_ON").text

        oriname = []
        if entry.find_all("NAME_ORIGINAL_SCRIPT"):
            for orn in entry.find_all("NAME_ORIGINAL_SCRIPT"):
                oriname.append(orn.text)

        comment = ""
        if entry.find("COMMENTS1"):
            comment = entry.find("COMMENTS1").text.strip()

        alias = []
        for ori in oriname:
            alias.append(ori)
        for enali in entry.find_all("ENTITY_ALIAS"):
            if enali.find("ALIAS_NAME"):
                if enali.find("ALIAS_NAME").text:
                    alias.append(enali.find("ALIAS_NAME").text.strip())

        nationality = []
        address = []
        for adrs in entry.find_all("ENTITY_ADDRESS"):
            adr_ob = {"completed_address": "", "zip": "", "country": ""}
            comp_adr = ""

            if adrs.find("STREET"):
                if adrs.find("STREET").text:
                    comp_adr = comp_adr + "," + adrs.find("STREET").text
            if adrs.find("CITY"):
                if adrs.find("CITY").text:
                    comp_adr = comp_adr + "," + adrs.find("CITY").text
            if adrs.find("STATE_PROVINCE"):
                if adrs.find("STATE_PROVINCE").text:
                    comp_adr = comp_adr + "," + adrs.find("STATE_PROVINCE").text
            if adrs.find("ZIP_CODE"):
                if adrs.find("ZIP_CODE").text:
                    comp_adr = comp_adr + "," + adrs.find("ZIP_CODE").text
                    adr_ob["zip"] = adrs.find("ZIP_CODE").text
            if adrs.find("COUNTRY"):
                if adrs.find("COUNTRY").text:
                    comp_adr = comp_adr + "," + adrs.find("COUNTRY").text
                    adr_ob["country"] = adrs.find("COUNTRY").text
                    nationality.append(adr_ob["country"])
                    
            comp_adr = comp_adr.strip().strip(",").strip()
            adr_ob["completed_address"] = comp_adr

            if adr_ob["completed_address"] or adr_ob["zip"] or adr_ob["country"]:
                address.append(adr_ob)
        
        if address == []:
            address.append({"completed_address":"","country" : ""})
        if alias == []:
           alias = alin_gen(name)

        out_obj["uid"] =  hashlib.sha256(((dataid +"UN SANCTIONS").lower()).encode()).hexdigest()
        out_obj["name"] = name
        out_obj["list_type"] = lstype
        out_obj["sanctions_details"] = {
            "un_list_type": unlstype,
            "refrence_no"   : refno,
            # "listed_on" : listname
        }
        out_obj["country"] = nationality
        out_obj["address"] = address
        out_obj["alias"] = alias
        out_obj["entity_details"] = {}
        out_obj["nns_status"] = False
        out_obj["last_updated"] = last_updated_string
        out_obj["comment"] = comment.strip()
        out_obj["list_id"] = "UN_S10005"
        out_obj["sanction_list"] = {
                "sl_authority": "United Nations Security Council",
                "sl_url": "https://www.un.org/securitycouncil/content/un-sc-consolidated-list",
                "watch_list": "Global Watchlists",
                "sl_host_country": "International",
                "sl_type": "Sanctions",
                "sl_source": "United Nations Security Council Consolidated List",
                "sl_description" : "The Consolidated List includes all individuals and entities subject to measures imposed by the Security Council."
        }

        out_list.append(out_obj)

    

    #NOTE:Saving outputfile localy 
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile,ensure_ascii=False)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile,ensure_ascii=False)

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
    new_list = copy.deepcopy(out_list)

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
                "dag" : "UN-SANCTIONS",
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
            passing = f"{today_date.ctime()},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(pass_first)
            outfile.write(passing)

#NOTE : Method to upload files in s3 bucket (Bhavesh Chavda)
def UploadfilestTos3():
    try:
        print("uploading files to s3")
        s3 = boto3.client('s3')
        s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"UN-SANCTIONS/original/{input_filename}")
        s3.upload_file(f'{op_path}/{output_filename}',"sams-scrapping-data",f"UN-SANCTIONS/parced/{output_filename}")
        s3.upload_file(f'{dp_path}/{diffrance_filename}',"sams-scrapping-data",f"UN-SANCTIONS/diffrance/{diffrance_filename}")
        s3.upload_file(f'{rm_path}/{removed_filename}',"sams-scrapping-data",f"UN-SANCTIONS/removed/{removed_filename}")
        s3.upload_file(f'{lp_path}',"sams-scrapping-data","UN-SANCTIONS/UN-SANCTIONS-logfile.csv")
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