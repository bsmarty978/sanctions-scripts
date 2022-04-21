from bs4 import BeautifulSoup
import json
from datetime import datetime as dt
from datetime import timedelta,date
import hashlib
import requests
import copy
from hijri_converter import Hijri
from deep_translator import GoogleTranslator
import boto3
import time
import traceback
import os
from os.path import exists

#NOTE: Object for output json file
out_obj = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = dt.now().strftime("%Y-%m-%dT%H:%M:%S")

input_filename  = f'seco-xml-{today_date.day}-{today_date.month}-{today_date.year}.xml'
output_filename = f'seco-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'diffrance-seco-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'removed-seco-json-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'seco-json-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'

#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/SECO/"
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}seco-logfile.csv"



#NOTE : Thhis method downloads the xml file
def xmlfiledownloder():
    URL = "https://www.sesam.search.admin.ch/sesam-search-web/pages/downloadXmlGesamtliste.xhtml?lang=en&action=downloadXmlGesamtlisteAction"
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



#NOTE: This method process the downloaded xml file and genrates a output JSON file
def fileprocessor():
    with open(f'{ip_path}/{input_filename}', 'rb') as f:
        data = f.read()
    # with open(f'consolidated-list_2022-01-28.xml', 'rb') as f:
    #     data = f.read()
    
    Bs_data = BeautifulSoup(data, "xml")

    programs =  Bs_data.find_all("sanctions-program")
    sancPrograms = {}
    for prog in programs:
        s_set = prog.find("sanctions-set", {"lang" : "eng"}).get("ssid")
        sancPrograms[s_set] = {}
        sancPrograms[s_set]["programname"] = prog.find("program-name", {"lang" : "eng"}).text
        sancPrograms[s_set]["sanctions-set"] = prog.find("sanctions-set", {"lang" : "eng"}).text
        sancPrograms[s_set]["country"] = prog.find("program-key", {"lang" : "eng"}).text 
        sancPrograms[s_set]["origin"] =prog.find("origin").text
    
    place_data = {}
    for pla in Bs_data.find_all("place"):
        place_data[pla.get("ssid")] = {}
        for i in pla.findChildren():
            if not isEnglish(i.text):
                place_data[pla.get("ssid")][i.name] = langtranslation(i.text)
            else:
                place_data[pla.get("ssid")][i.name] = i.text

    global total_profile_available 
    entries = Bs_data.find_all("target")
    # ssid_list = []
    # for entry in entries:
    #     if entry.get("ssid") not in ssid_list:
    #         ssid_list.append(entry.get("ssid"))

    ssid_list = []
    for entry in entries:
        pass_obj = {}
        ssid = entry.get("ssid")
        if ssid not in ssid_list:
            ssid_list.append(ssid)
            ltype = entry.findChildren(recursive=False)[0].name
            sanc_set_id = entry.get("sanctions-set-id")

            sanction_Details = {}
            sanction_Details["progaramList"] = []
            sanction_Details["progaramList"].append(sancPrograms[sanc_set_id].get('programname'))
            sanction_Details["country"] = sancPrograms[sanc_set_id].get('country')
            sanction_Details["sanctions-set"] = sancPrograms[sanc_set_id].get('sanctions-set')
            sanction_Details["origin"] = sancPrograms[sanc_set_id].get('origin')


            if ltype == "object":
                list_types = entry.findChildren(recursive=False)[0].get("object-type")
            elif ltype == "foreign-identifier":
                list_types = entry.findChildren(recursive=False)[1].name
            else:
                list_types = ltype  

            identity = entry.find("identity",{"main" : "true"})
            meta_name = {}
            # name = ""
            # aliases_obj = {}
            # aliases = []
            # for namer in identity.find_all("name-part"):
            #     # if namer.find("value").text:
            #     name =  name + " " + namer.find("value").text
            #     alis_count = 0
            #     for alis in namer.find_all("spelling-variant"):
            #         try:
            #             aliases_obj[alis_count] = aliases_obj[alis_count] + " " + alis.text
            #         except KeyError:
            #             aliases_obj[alis_count] = ""
            #             aliases_obj[alis_count] = aliases_obj[alis_count] + " " + alis.text
            #         alis_count += 1

            # name = name.strip()
            # for ali in aliases_obj:
            #     aliases.append(aliases_obj[ali].strip())

            for namer in identity.find_all("name-part"):
                name_type = namer.get("name-part-type")
                if name_type != "whole-name":
                    meta_name[name_type] = {}
                    meta_name[name_type]["value"] = namer.find("value").text
                    meta_name[name_type]["varis"] = []
                    for alis in namer.find_all("spelling-variant"):
                        meta_name[name_type]["varis"].append(alis.text)
                else:
                    n_type = namer.find_parent("name").get("name-type")
                    if n_type == "primary-name":
                        try:
                            meta_name[name_type]["value"] = namer.find("value").text
                        except:
                            meta_name[name_type] = {}
                            meta_name[name_type]["value"] = namer.find("value").text
                        for alis in namer.find_all("spelling-variant"):
                            try:
                                meta_name[name_type]["varis"].append(alis.text)
                            except:
                                meta_name[name_type]["varis"] = []
                                meta_name[name_type]["varis"].append(alis.text)
                    else:
                        try:
                            meta_name[name_type]["varis"].append(namer.find("value").text)   
                        except:
                            try:
                                meta_name[name_type]["varis"] = []
                                meta_name[name_type]["varis"].append(namer.find("value").text)   
                            except:
                                meta_name[name_type] = {}
                                meta_name[name_type]["varis"] = []
                                meta_name[name_type]["varis"].append(namer.find("value").text)
                        for alis in namer.find_all("spelling-variant"):
                            try:
                                meta_name[name_type]["varis"].append(alis.text)
                            except:
                                meta_name[name_type]["varis"] = []
                                meta_name[name_type]["varis"].append(alis.text)
                    
            new_nstr = ""
            new_aliases = []
            new_aobj = {}
            whole_bug = 0
            if meta_name.get("given-name"):
                whole_bug +=1
                new_nstr = new_nstr + " " + meta_name["given-name"]["value"]
                ali_c = 0
                for alis in meta_name["given-name"]["varis"]:
                    try:
                        new_aobj[ali_c] = new_aobj[ali_c] + " " + alis
                    except KeyError:
                        new_aobj[ali_c] = ""
                        new_aobj[ali_c] = new_aobj[ali_c] + " " + alis
                    ali_c += 1
            if meta_name.get("father-name"):
                whole_bug +=1
                new_nstr = new_nstr + " " + meta_name["father-name"]["value"]
                ali_c = 0
                for alis in meta_name["father-name"]["varis"]:
                    try:
                        new_aobj[ali_c] = new_aobj[ali_c] + " " + alis
                    except KeyError:
                        new_aobj[ali_c] = ""
                        new_aobj[ali_c] = new_aobj[ali_c] + " " + alis
                    ali_c += 1
            if meta_name.get("family-name"):
                whole_bug +=1
                new_nstr = new_nstr + " " + meta_name["family-name"]["value"]
                ali_c = 0
                for alis in meta_name["family-name"]["varis"]:
                    try:
                        new_aobj[ali_c] = new_aobj[ali_c] + " " + alis
                    except KeyError:
                        new_aobj[ali_c] = ""
                        new_aobj[ali_c] = new_aobj[ali_c] + " " + alis
                    ali_c += 1
            if whole_bug == 0:
                if meta_name.get("whole-name"):
                    new_nstr = meta_name.get("whole-name").get("value")
                    ali_c = 0
                    if meta_name["whole-name"].get("varis"):
                        for alis in meta_name["whole-name"]["varis"]:
                            try:
                                new_aobj[ali_c] = new_aobj[ali_c] + " " + alis
                            except KeyError:
                                new_aobj[ali_c] = ""
                                new_aobj[ali_c] = new_aobj[ali_c] + " " + alis
                            ali_c += 1
            try:
                new_nstr = new_nstr.strip()
                for ali in new_aobj:
                    new_aliases.append(new_aobj[ali].strip())
            except:
                print(f"<><>><>{ssid}")

            dobs = []
            for dob in entry.find_all("day-month-year",{"calendar":"Gregorian"}):
                idate = dob.get("day")
                imonth = dob.get("month")
                iyear = dob.get("year")
                if not idate and not imonth:
                    dobs.append(f"{iyear}")
                elif not idate:
                    dobs.append(f"{imonth}/{iyear}")
                else:
                    dobs.append(f"{idate}/{imonth}/{iyear}")

            if dobs == []:
                if entry.find_all("day-month-year",{"calendar":"Hijri"}):
                    print("Hijri Only DOBS found")
                    hijriday = entry.find_all("day-month-year",{"calendar":"Gregorian"})
                    idate = hijriday.get("day")
                    imonth = hijriday.get("month")
                    iyear = hijriday.get("year")
                    gdob = Hijri(int(idate), int(imonth), int(iyear)).to_gregorian()
                    if not idate and not imonth:
                        gdob = Hijri(1, 1, int(idate)).to_gregorian()
                        dobs.append(f"{gdob.year}")
                    elif not idate:
                        gdob = Hijri(1, int(imonth), int(idate)).to_gregorian()
                        dobs.append(f"{gdob.month}/{gdob.year}")
                    else:
                        gdob = Hijri(int(idate), int(imonth), int(idate)).to_gregorian()
                        dobs.append(f"{gdob.day}/{gdob.month}/{gdob.year}")
            else:
                dobs = list(set(dobs))

            pobs = []
            for pob in identity.find_all("place-of-birth"):
                pid = pob.get("place-id")
                pobs.append(place_data[pid])
            
            addr_list = []
            country = []
            for adr in identity.find_all("address"):
                adr_sin = {}
                trstr = ""
                for i in adr.findChildren():
                    trstr = trstr + "," + i.text
                if trstr:
                    trstr = trstr.strip(',')
                    if not isEnglish(trstr):
                        trstr = langtranslation(trstr)
                trstr = trstr.strip(',')

                apid = adr.get("place-id")
                for j in place_data[apid]:
                    trstr = trstr + "," + place_data[apid][j]
                
                adr_sin['complete-address'] = trstr.strip(",")

                if place_data[apid].get("zip-code"):
                    adr_sin['zip_code'] = place_data[apid]["zip-code"]
                else:
                    adr_sin['zip_code'] = ""

                if place_data[apid].get("location"):
                    adr_sin['location'] = place_data[apid]["location"]
                else:
                    adr_sin['location'] = ""

                if place_data[apid].get("country"):
                    adr_sin['country'] = place_data[apid]["country"]
                    if place_data[apid]["country"] not in country:
                        country.append(place_data[apid]["country"])
                else:
                    adr_sin['country'] = ""
                
                addr_list.append(adr_sin)
            
            ids_docs = {}
            for id_doc in identity.find_all("identification-document"):
                ids_docs[id_doc.get("document-type")] = []
                ids_docs[id_doc.get("document-type")].append(id_doc.find("number").text)
            
            justification = []
            other_infos = []
            if list_types != "vessel":
                for justi in entry.find(f"{list_types}").find_all("justification"):
                    justification.append(justi.text)
                for oinfo in entry.find(f"{list_types}").find_all("other-information"):
                    other_infos.append(oinfo.text)
            else:
                for justi in entry.find("object").find_all("justification"):
                    justification.append(justi.text)
                for oinfo in entry.find("object").find_all("other-information"):
                    other_infos.append(oinfo.text)
            
            uid = hashlib.sha256(((ssid +"SANCTIONS SECO list").lower()).encode()).hexdigest()
            
            pass_obj["uid"] = uid
            pass_obj["name"] = new_nstr
            pass_obj["alias_name"] = new_aliases
            pass_obj["country"] = country
            pass_obj["list_type"] = list_types
            pass_obj["last_updated"] = last_updated_string
            if list_types=="individual":
                pass_obj["individual_details"] = {
                    "date_of_birth" : dobs,
                    "gender" : "",
                    "birthplace" :pobs
                }
            elif list_types=="entity":
                pass_obj["entity_details"] = {}
            elif list_types=="vessel":
                pass_obj["vessel_details"] = {}
                if "IMO" in other_infos[0]:
                    pass_obj["vessel_details"]["IMO"] = other_infos[0].split(":")[-1].strip()

            pass_obj["nns_status"] =  "False"
            pass_obj["address"] = addr_list
            pass_obj["sanction_Details"] = sanction_Details
            pass_obj["documents"] = ids_docs
            if len(justification)>0:
                pass_obj["comment"] =  justification[0]
            else:
                pass_obj["comment"] =  ""
            
            pass_obj["list_type"] = pass_obj["list_type"].title()  #NOTE : New Update for list_type  
            pass_obj["list_id"] = "CHE_S10004" 
            pass_obj["sanction_list"] = {
                "sl_authority": "State Secretariat for Economic Affairs SECO, Switzerland",
                "sl_url": "https://www.seco.admin.ch/seco/en/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos.html",
                "sl_host_country": "Switzerland",
                "watch_list": "European Watchlists",
                "sl_type": "Sanctions",
                "sl_source": "State Secretariat for Economic Affairs Sanctions List",
                "sl_description": "The list of designated people and organizations by State Secretariat for Economic Affairs SECO, Switzerland."
            }
            

            out_obj.append(pass_obj)

    total_profile_available= len(ssid_list)

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
                "dag" : "SECO",
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
        s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"seco/original/{input_filename}")
        s3.upload_file(f'{op_path}/{output_filename}',"sams-scrapping-data",f"seco/parced/{output_filename}")
        s3.upload_file(f'{dp_path}/{diffrance_filename}',"sams-scrapping-data",f"seco/diffrance/{diffrance_filename}")
        s3.upload_file(f'{rm_path}/{removed_filename}',"sams-scrapping-data",f"seco/removed/{removed_filename}")
        s3.upload_file(f'{lp_path}',"sams-scrapping-data","seco/seco-logfile.csv")
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
    