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

dag_name = "uk-sanc"

input_filename  = f'{dag_name}-xml-{today_date.day}-{today_date.month}-{today_date.year}.xml'
output_filename = f'{dag_name}-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'diffrance-{dag_name}-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'removed-{dag_name}-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-json-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'

#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/UK-SANCTIONS/"
# root = ""
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def alin_gen(nmstri):
    out = []
    nm = nmstri.split(' ')
    if len(nm)==2:
        out.append(nm[1].strip() + " " + nm[0].strip())
    if len(nm) > 2:
        out.append(nm[0].strip() + " " + nm[2].strip())
        out.append(nm[2].strip() + " " + nm[0].strip())
        out.append(nm[1].strip() + " " + nm[2].strip())
        out.append(nm[2].strip() + " " + nm[1].strip())
    else:
        out.append(nmstri)
    return out


def xmlfiledownloder():
    # URL = "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1058704/UK_Sanctions_List.xml"
    URL = "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1059992/UK_Sanctions_List.xml"
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

    Bs_data = BeautifulSoup(data, "xml")
    entries = Bs_data.find_all("Designation")
    global total_profile_available
    total_profile_available = len(entries)

    for entry in entries:
        out_obj = {}
        uid = entry.find('UniqueID').text.strip()
        ofsi = ""
        unrefno = ""
        if entry.find('OFSIGroupID'):
            ofsi = entry.find('OFSIGroupID').text.strip()
        if entry.find('UNReferenceNumber'):
            unrefno = entry.find('UNReferenceNumber').text.strip()

        # all_names = [i.text.strip() for i in entry.find_all("Name6")]

        all_names = []
        for n in entry.find_all("Name"):
            nmstr = ""
            if n.find('Name1'):
                nmstr = nmstr.strip() + " " + n.find('Name1').text.strip()
            if n.find('Name2'):
                nmstr = nmstr.strip() + " " + n.find('Name2').text.strip()
            if n.find('Name3'):
                nmstr = nmstr.strip() + " " + n.find('Name3').text.strip()
            if n.find('Name4'):
                nmstr = nmstr.strip() + " " + n.find('Name4').text.strip()
            if n.find('Name5'):
                nmstr = nmstr.strip() + " " + n.find('Name5').text.strip()
            if n.find('Name6'):
                nmstr = nmstr.strip() + " " + n.find('Name6').text.strip()
            if nmstr.strip():
                all_names.append(nmstr.strip())

        try:
            name = all_names[0]
        except:
            print(f"Can not Find Name in source for : {uid}")
            # break

        alias = []
        for j in range(1,len(all_names)):
            alias.append(all_names[j])

        if entry.find("NameNonLatinScript"):
            if entry.find("NameNonLatinScript").text:
                alias.append(entry.find("NameNonLatinScript").text)

        regime = ""
        sancimpo = ""
        comment = ""
        if entry.find("RegimeName"):
            regime = entry.find("RegimeName").text.strip()
        lstype = entry.find("IndividualEntityShip").text.strip()
        if lstype == "Ship":
            lstype = "Vessel"
        if entry.find("SanctionsImposed"):
            sancimpo = entry.find("SanctionsImposed").text.strip()
        if entry.find("OtherInformation"):
            comment = entry.find("OtherInformation").text.strip()
        if entry.find("UKStatementofReasons"):
            if entry.find("UKStatementofReasons").text.strip():
                comment = comment+ "\n" + entry.find("UKStatementofReasons").text.strip()

        address = []
        country = []
        for ad in entry.find_all("Address"):
            comp_adr = ""
            cont = ""
            if ad.find("AddressCountry"):
                cont = ad.find("AddressCountry").text.strip()
                if cont:
                    country.append(cont)
            if ad.find("AddressLine1"):
                comp_adr = comp_adr.strip().strip(",").strip() + "," + ad.find("AddressLine1").text.strip()
            if ad.find("AddressLine2"):
                comp_adr = comp_adr.strip().strip(",").strip() + "," + ad.find("AddressLine2").text.strip()
            if ad.find("AddressLine3"):
                comp_adr = comp_adr.strip().strip(",").strip() + "," + ad.find("AddressLine3").text.strip()
            if ad.find("AddressLine4"):
                comp_adr = comp_adr.strip().strip(",").strip() + "," + ad.find("AddressLine4").text.strip()
            if ad.find("AddressLine5"):
                comp_adr = comp_adr.strip().strip(",").strip() + "," + ad.find("AddressLine5").text.strip()
            if ad.find("AddressLine6"):
                comp_adr = comp_adr.strip().strip(",").strip() + "," + ad.find("AddressLine6").text.strip()
            comp_adr = comp_adr.strip().strip(",").strip()

            if comp_adr or cont:
                address.append({"complete_address":comp_adr,"country":cont})
        if address==[]:
            address.append({"complete_address":"","country":""})
        country = list(set(country))

        if alias == []:
           alias = alin_gen(name)


        phones = []
        for ph in entry.find_all("PhoneNumber"):
            if ph.text.strip():
                phones.append(ph.text.strip())

        mails = []
        for mail in entry.find_all("EmailAddress"):
            if mail.text.strip():
                mails.append(mail.text.strip())

        webs = []
        for web in entry.find_all("Website"):
            if web.text.strip():
                webs.append(web.text.strip())
        
        docs = {}


        out_obj["uid"] =  hashlib.sha256(((uid+ofsi+unrefno+"UK SANCTIONS").lower()).encode()).hexdigest()
        out_obj["name"] = name
        out_obj["list_type"] = lstype
        out_obj["alias_name"] = alias

        if lstype == "Individual":
            dobs = []
            for dob in entry.find_all("DOB"):
                d = dob.text.strip()
                if d.startswith("dd/mm/"):
                    d = d.split("dd/mm/")[-1].strip()
                elif d.startswith("dd/"):
                    d = d.split("dd/")[-1].strip()
                dobs.append(d)

            nationality = []
            for na in entry.find_all("Nationality"):
                if na.text.strip():
                    nationality.append(na.text.strip())
            nationality = list(set(nationality))

            pos = []
            for po in entry.find_all("Position"):
                if po.text.strip():
                    pos.append(po.text.strip())

            pobs = []
            for lo in entry.find_all("Location"):
                comp = ""
                co = ""
                if lo.find("TownOfBirth"):
                    comp = lo.find("TownOfBirth").text.strip()
                if lo.find("CountryOfBirth"):
                    co = lo.find("CountryOfBirth").text.strip()
                if comp or co:
                    pobs.append({"complete_address":comp,"country":co})
            
            passports = []
            for pas in entry.find_all("PassportNumber"):
                if pas.text.strip():
                    passports.append(pas.text.strip())

            nids = []
            for nid in entry.find_all("NationalIdentifierNumber"):
                if nid.text.strip():
                    nids.append(nid.text.strip())
            
            docs["passport"] = passports
            docs["other_id"] = nids

            out_obj["individual_details"] = {
                "date_of_birth" : dobs,
                "placeofbirth" : pobs,
                # "passport": passports,
                "nationality" : nationality,
                "designation" : pos,
                "phone_number" :phones,
                "website" : webs,
                "email" :mails
            } 

        elif lstype == "Entity":
            etype = ""
            if entry.find("TypeOfEntity"):
                etype = entry.find("TypeOfEntity").text.strip()
            subs = []
            for subsdi in entry.find_all("Subsidiary"):
                if subsdi.text.strip():
                    subs.append(subsdi.text.strip())
            out_obj["entity_details"] = {
                "type_of_entity" : etype,
                "website" : webs,
                "subsidiary" : subs,
                "phone_number" :phones,
                "email" :mails
            }

        elif lstype == "Vessel":
            imos = []
            for im in entry.find_all("IMONumber"):
                if im.text.strip():
                    imos.append(im.text.strip())
            curop = ""
            if entry.find("CurrentOwnerOperator"):
                curop = entry.find("CurrentOwnerOperator").text.strip()

            flags = []
            if entry.find("CurrentBelievedFlagOfShip"):
                if entry.find("CurrentBelievedFlagOfShip").text.strip():
                    flags.append(entry.find("CurrentBelievedFlagOfShip").text.strip())
            if entry.find("PreviousFlag"):
                if entry.find("PreviousFlag").text.strip():
                    flags.append(entry.find("PreviousFlag").text.strip())
            
            stype = ""
            if entry.find("TypeOfShip"):
                stype = entry.find("TypeOfShip").text.strip()

            ton = ""
            if entry.find("TonnageOfShip"):
                ton = entry.find("TonnageOfShip").text.strip()

            slen = ""
            if entry.find("LengthOfShipDetails"):
                slen = entry.find("LengthOfShipDetails").text.strip()

            builtyear = ""
            if entry.find("YearsBuilt"):
                builtyear = entry.find("YearsBuilt").text.strip()

            out_obj["vessel_details"] = {
                "IMONumber" : imos,
                "currentOwnerOperator" : curop,
                "flags" : flags,
                "vessel_type" : stype,
                "tonnage_of_ship" : ton,
                "length_of_ship" : slen,
                "yearbuilt" : builtyear,
            }


        out_obj["documet"] = docs
        out_obj["sanctions_details"] = {
            "RegimeName" : regime,
            "SanctionsImposed" : sancimpo
        }
        out_obj["country"] = country
        out_obj["address"] = address
        out_obj["last_updated"] = last_updated_string
        out_obj["nns_status"] = False
        out_obj["comment"] = comment
        out_obj["sanction_list"] = {
            "sl_authority": "United Kingdom Government",
            "sl_url": "https://www.gov.uk/government/publications/the-uk-sanctions-list",
            "watch_list": "European Watchlistss",
            "sl_host_country": "United Kingdom",
            "sl_type": "Sanctions",
            "list_id" : "UK_S10053",
            "sl_source": "United Kingdom Consolidated Sanctions List, United Kingdom",
            "sl_description": "The UK government publishes the UK Sanctions List, which provides details of those designated under regulations made under the Sanctions Act.. "          
        }
        out_list.append(out_obj)


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
    if len(new_list)==0:
        removed_profiles = []
        raise ValueError("Error : Data Parsing Error.... fix it quick ⚒️")
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
        s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"UK-SANCTIONS/original/{input_filename}")
        s3.upload_file(f'{op_path}/{output_filename}',"sams-scrapping-data",f"UK-SANCTIONS/parced/{output_filename}")
        s3.upload_file(f'{dp_path}/{diffrance_filename}',"sams-scrapping-data",f"UK-SANCTIONS/diffrance/{diffrance_filename}")
        s3.upload_file(f'{rm_path}/{removed_filename}',"sams-scrapping-data",f"UK-SANCTIONS/removed/{removed_filename}")
        s3.upload_file(f'{lp_path}',"sams-scrapping-data",f"UK-SANCTIONS/{lp_path}")
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