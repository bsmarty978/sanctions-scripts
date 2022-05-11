from bs4 import BeautifulSoup
import json
from datetime import datetime as dt
from datetime import timedelta,date
import hashlib
import requests
import copy
import os
from os.path import exists
import boto3

last_updated_string = dt.now().strftime("%Y-%m-%dT%H:%M:%S")
base_individual_obj = {
        "uid": "",
        "name": "",
        "alias_name": [],
        "designation" : [],
        "country": [],
        "list_type": "",
        "last_updated": "",
        "individual_details": {},
        "entity_details" : {},
        "sanction_Details" : {},
        "nns_status": "False",
        "address": [],
        "documents": {},
        "comment": "",
        "sanction_list": {
            "sl_authority": "Office of Foreign Assets Control",
            "sl_url": "https://home.treasury.gov/policy-issues/office-of-foreign-assets-control-sanctions-programs-and-information",
            "sl_host_country": "United States of America",
            "sl_type": "NON-SDN(Sanctions)",
            "sl_source": "Office Of Foreign Assets Control Sanction List(Covers 73 Lists)",
            "sl_description": "The Office of Foreign Assets Control(OFAC) is a financial intelligence and enforcement agency of the U.S. Treasury Department."
        }
    }


#NOTE: Object for output json file
out_obj = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)

input_filename  = f'nonsdn-xml-{today_date.day}-{today_date.month}-{today_date.year}.xml'
output_filename = f'nonsdn-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'diffrance-nonsdn-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'removed-nonsdn-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'nonsdn-json-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'

#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/OFAC-NONSDN/"
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}ofac-nonsdn-logfile.csv"



#NOTE : Thhis method downloads the xml file
def xmlfiledownloder():
    # URL = "https://www.treasury.gov/ofac/downloads/sdn.xml" #NOTE: URL for sdn 
    URL = "https://www.treasury.gov/ofac/downloads/consolidated/consolidated.xml"
    response = requests.get(URL)
    try:
        with open(f'{ip_path}/{input_filename}', 'wb') as file:
            file.write(response.content)
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename}', 'wb') as file:
            file.write(response.content)


#NOTE: This method process the downloaded xml file and genrates a output JSON file
def fileprocessor():
    with open(f'{ip_path}/{input_filename}', 'rb') as f:
        data = f.read()
    
    Bs_data = BeautifulSoup(data, "xml")
    entites =  Bs_data.find_all("sdnEntry")

    global total_profile_available 
    total_profile_available= len(entites)
    print(f'>>>>>>Total Profiles: {total_profile_available}')


    #NOTE: Tranform xml data into Json data:
    sda_data_obj = []
    for i in entites:
        sdn_single_obj = {}
        sdn_single_obj["uid"] = i.find("uid").text
        if i.find("firstName"):
            sdn_single_obj["firstName"] = i.find("firstName").text
        else:
            sdn_single_obj["firstName"] = None
        sdn_single_obj["lastName"] = i.find("lastName").text
        sdn_single_obj["sdnType"] = i.find("sdnType").text
        sdn_single_obj["programList"] = [j.text for j in i.find_all("program")]
        
        if i.find("remarks"):
            sdn_single_obj["remarks"] = i.find("remarks").text
        else:
            sdn_single_obj["remarks"] = None

        if i.find("title"):
            sdn_single_obj["title"] = i.find("title").text
        else:
            sdn_single_obj["title"] = None


        akaList = []
        for k in i.find_all("aka"):
            aka_obj = {}
            for aka in k.findChildren():
                aka_obj[aka.name] = aka.text
            akaList.append(aka_obj)
            # akaList.append({
            #     "uid" : k.find("uid").text,
            #     "type" : k.find("type").text,
            #     "category" : k.find("category").text,
            #     "lastName" : k.find("lastName").text
            # })
        sdn_single_obj["akaList"] = akaList
        
        addressList = []
        for l in i.find_all("address"):
            adr_obj = {}
            for adr in l.findChildren():
                adr_obj[adr.name] = adr.text
            addressList.append(adr_obj)        
        sdn_single_obj["addressList"] = addressList
        

        nationalityList = []
        if i.find_all("nationality"):
            for c in i.find_all("nationality"):
                nationalityList.append(c.find("country").text)
        sdn_single_obj["nationalityList"] = nationalityList

        citizenshipList = []
        if i.find_all("citizenship"):
            for c in i.find_all("citizenship"):
                citizenshipList.append(c.find("country").text)
        sdn_single_obj["citizenshipList"] = citizenshipList


        dateOfBirthList = []
        if i.find_all("dateOfBirth"):
            for dob in i.find_all("dateOfBirth"):
                dateOfBirthList.append(dob.text)
        sdn_single_obj["dateOfBirthList"] = dateOfBirthList
        
        placeOfBirthList = []
        if i.find_all("placeOfBirth"):
            for pob in i.find_all("placeOfBirth"):
                placeOfBirthList.append(pob.text)
        sdn_single_obj["placeOfBirthList"] = placeOfBirthList

        idList = []
        if i.find_all("id"):
            for id in i.find_all("id"):
                id_obj = {}
                for data in id.findChildren():
                    id_obj[data.name] = data.text
                idList.append(id_obj)
        sdn_single_obj["idList"] = idList

        if i.find("vesselInfo"):
            sdn_single_obj["vesselInfo"] = {}
            for info in i.find("vesselInfo").findChildren():
                sdn_single_obj["vesselInfo"][info.name] = info.text
        else:
            sdn_single_obj["vesselInfo"] = {}
            
        sda_data_obj.append(sdn_single_obj)
        

    #NOTE:Tranforming to give JSON schema  
    # out_obj = []
    for entry in sda_data_obj:
        copy_obj = {}
        copy_obj["uid"] = hashlib.sha256(((entry["lastName"] +entry["uid"] +  "OFAC NON-SDN list").lower()).encode()).hexdigest()
        if entry["firstName"]:
            copy_obj["name"] = f'{entry.get("firstName")} {entry.get("lastName")}'
        else:
            copy_obj["name"] = entry["lastName"]

        copy_obj["alias_name"] = []
        if entry["akaList"]:
            # copy_obj["alias_name"] = []
            for ali in entry["akaList"]:
                if ali.get("firstName"):
                    copy_obj["alias_name"].append(f'{ali.get("lastName")} {ali.get("firstName")}')
                else:
                    copy_obj["alias_name"].append(f'{ali.get("lastName")}')
        else:
            if entry["firstName"]:  
                copy_obj["alias_name"].append(f'{entry.get("firstName")} {entry.get("lastName")}')
                copy_obj["alias_name"].append(f'{entry.get("lastName")} {entry.get("firstName")}')
            else:
                copy_obj["alias_name"].append(entry["lastName"])


        copy_obj["list_type"] = entry["sdnType"]
        copy_obj["last_updated"] = last_updated_string
        copy_obj["nns_status"] =  "False"

        copy_obj["sanction_Details"] = {}
        copy_obj["sanction_Details"]["programList"] = entry["programList"]
        # copy_obj["comment"] = entry["remarks"]
        if entry["title"]:
            copy_obj["designation"] = []
            copy_obj["designation"].append(entry["title"])

        if copy_obj["list_type"] == "Individual":
            copy_obj["individual_details"] = {}
            copy_obj["individual_details"]["date_of_birth"] = entry.get("dateOfBirthList")
            copy_obj["individual_details"]["place_of_birth"] = entry.get("placeOfBirthList")
            copy_obj["individual_details"]["gender"] = ""

        elif copy_obj["list_type"] == "Entity":
            copy_obj["entity_details"] = {}
        elif copy_obj["list_type"] == "Vessel":
            copy_obj["vessel_details"] = {}
            copy_obj["vessel_details"] = entry.get("vesselInfo")
        elif copy_obj["list_type"] == "Aircraft":
            copy_obj["aircraft_details"] = {}
        else:
            pass


        copy_obj["address"] = []
        copy_obj["country"] = []

        if entry["addressList"]:
            for addr in entry["addressList"]:
                adr_new_obj = {}
                del addr["uid"]
                comp_add = ""
                counter = 0
                for line in addr:
                    if counter == 0:
                        comp_add = addr[line]
                        counter += 1
                    else:
                        comp_add = comp_add + ',' + addr[line]
                adr_new_obj["complete_address"] = comp_add

                if "address1" in addr.keys():
                    del addr["address1"]
                if "address2" in addr.keys():
                    del addr["address2"]
                if "address3" in addr.keys():
                    del addr["address3"]
                if "postalCode" in addr.keys():
                    addr["zipCode"] = addr["postalCode"]
                    del addr["postalCode"]
                if "stateOrProvince" in addr.keys():
                    addr["state"] = addr["stateOrProvince"]
                    del addr["stateOrProvince"]

                adr_new_obj.update(addr)
                copy_obj["address"].append(adr_new_obj)

                copy_obj["country"].append(addr.get("country"))
        
        if copy_obj["address"] == []:
            copy_obj["address"] = [{
                "complete_address" : "",
                "country" : ""
            }]

        
        copy_obj["documents"] = {}
        if entry["idList"]:
            for info in entry["idList"]:
                if info["idType"] == "Gender":
                    copy_obj["individual_details"]["gender"] = info["idNumber"]
                elif info["idType"] == "Passport":
                    copy_obj["documents"][(info["idType"])] = []
                    copy_obj["documents"][(info["idType"])].append(info["idNumber"])
                else:
                    copy_obj["documents"][(info["idType"]).replace(".","").strip()] = info["idNumber"]
                # info_obj = info
                # if info_obj["uid"]:
                #     del info_obj["uid"]
                # copy_obj["documents"].append(info_obj)

        # copy_obj["vesselInfo"] = entry["vesselInfo"]
        copy_obj["list_id"] = "USA_S10001" 
        copy_obj["sanction_list"] = {
            "sl_authority": "Office of Foreign Assets Control",
            "sl_url": "https://home.treasury.gov/policy-issues/office-of-foreign-assets-control-sanctions-programs-and-information",
            # "sl_url": "https://home.treasury.gov/policy-issues/financial-sanctions/specially-designated-nationals-list-data-formats-data-schemas", #for SDN list
            "watch_list" : "Global Watchlists",
            "sl_host_country": "United States of America",
            "sl_type": "NON-SDN(Sanctions)",
            # "sl_type": "Specially Designated Nationals", # For SDN lists
            "sl_source": "Office Of Foreign Assets Control Consolidated Sanctions List, USA (Covers 73 List)",
            "sl_description": "The Office of Foreign Assets Control(OFAC) is a financial intelligence and enforcement agency of the U.S. Treasury Department."
        }


        out_obj.append(copy_obj)

    #NOTE:Saving outputfile localy 
    try:
        with open(f'{op_path}/{output_filename}', "w") as outfile:
            json.dump(out_obj, outfile)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w") as outfile:
            json.dump(out_obj, outfile)


def CompareDocument():
    try:
        with open(f'{op_path}/{old_output_filename}', 'rb') as f:
            data = f.read()
    except Exception as e:
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
                "dag" : "OFAC-NON-SDN",
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
        with open(f'{dp_path}/{diffrance_filename}', "w") as outfile:
            json.dump(new_profiles+updated_profiles, outfile)
    except FileNotFoundError:
        os.mkdir(dp_path)
        with open(f'{dp_path}/{diffrance_filename}', "w") as outfile:
            json.dump(new_profiles+updated_profiles, outfile)

    try:
        with open(f'{rm_path}/{removed_filename}', "w") as outfile:
            json.dump(removed_profiles, outfile)
    except FileNotFoundError:
        os.mkdir(rm_path)
        with open(f'{rm_path}/{removed_filename}', "w") as outfile:
            json.dump(removed_profiles, outfile)

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
        s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"OFAC/nonsdn/original/{input_filename}")
        s3.upload_file(f'{op_path}/{output_filename}',"sams-scrapping-data",f"OFAC/nonsdn/parced/{output_filename}")
        s3.upload_file(f'{dp_path}/{diffrance_filename}',"sams-scrapping-data",f"OFAC/nonsdn/diffrance/{diffrance_filename}")
        s3.upload_file(f'{rm_path}/{removed_filename}',"sams-scrapping-data",f"OFAC/nonsdn/removed/{removed_filename}")
        s3.upload_file(f'{lp_path}',"sams-scrapping-data","OFAC/nonsdn/ofac-nonsdn-logfile.csv")
        print("uploaded files to s3")      
    except Exception as e:
        print("------------------🔴ALERT🔴------------------------")
        print("Can not upload files to s3")
        print("Exception : " , e)
        print("----------------------------------------------------")

try:
    xmlfiledownloder()
    fileprocessor()
    CompareDocument()
    UploadfilestTos3()
except Exception as e:
    print("------------------🔴ALERT🔴------------------------")
    print("Exception : " , e)
    print("----------------------------------------------------")

    
