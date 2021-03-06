import requests
import json
import os
import datetime
import hashlib
import pycountry
import boto3
import copy
from os.path import exists

class interpoleentity():
    try:
        final_list = []
        input_list = []
        date = datetime.date.today()
        yesterday = date - datetime.timedelta(days=1)
        last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        # root = ""
        root = "/home/ubuntu/sanctions-scripts/UN-INTERPOL-EN/"
        inputpath = f'{root}InputFiles'
        outputpath = f'{root}OutputFiles'
        Differencepath = f'{root}DifferenceFiles'
        rmpath = f'{root}RemovedFiles'
        lp_path = f"{root}un_entity_log.csv"
        os.mkdir(rmpath)
        os.mkdir(inputpath)
        os.mkdir(outputpath)
        os.mkdir(Differencepath)
    except:
        pass

    def uploadToS3(self):
        try:
            print("uploading files to s3")
            s3 = boto3.client('s3')
            s3.upload_file(f'{self.inputpath}/{self.date}_input_UN_Entity.json', "sams-scrapping-data",
                           f"UN-INTERPOL-ENTITY/Original/{self.date}_input_UN_Entity.json")
            s3.upload_file(f'{self.outputpath}/{self.date}_output_UN_Entity.json', "sams-scrapping-data",
                           f"UN-INTERPOL-ENTITY/Parced/{self.date}_output_UN_Entity.json")
            s3.upload_file(f'{self.Differencepath}/{self.date}_Difference_Output_UN_Entity.json', "sams-scrapping-data",
                           f"UN-INTERPOL-ENTITY/Diffrance/{self.date}_Difference_Output_UN_Entity.json")
            s3.upload_file(f'{self.rmpath}/{self.date}_RemovedData_File_UN_Entity.json', "sams-scrapping-data",
                    f"UN-INTERPOL-ENTITY/Removed/{self.date}_RemovedData_File_UN_Entity.json")       
            s3.upload_file(f'{self.lp_path}', "sams-scrapping-data",
                f"UN-INTERPOL-ENTITY/un_entity_log.csv")
            print("uploaded files to s3")
        except Exception as e:
            print(e)

    def compare(self):
        try:
            with open(f'{self.outputpath}/{self.yesterday}_output_UN_Entity.json', 'rb') as f:
                data = f.read()
        except:
            # print("---------------------Alert--------------------------")
            print(f"There is not any old file for date: {self.yesterday.ctime()}")
            # print("----------------------------------------------------")
            data = "No DATA"
        old_list = []
        if data != "No DATA":
            old_list = json.loads(data)
        new_list = copy.deepcopy(self.final_list)

        new_profiles = []
        removed_profiles = []
        updated_profiles = []

        old_dict = {}
        for val in old_list:
            old_dict[val["uid"]] = val

        new_uid_list = []
        for val1 in new_list:
            new_uid = val1["uid"]
            new_uid_list.append(new_uid)
            if new_uid in old_dict.keys():
                # print("Already in List")
                for i in val1:
                    if i != "last_updated":
                        try:
                            if val1[i] != old_dict[val1["uid"]][i]:
                                print(f"Updataion Detected on: {val1['uid']} for: {i}")
                                updated_profiles.append(val1)
                                break
                        except:
                            print(f"Updataion Detected on: {val1['uid']} for: {i}")
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
            raise ValueError("Error : Data Parsing Error.... fix it quick ??????")
        #NOTE: ---------- META LOg FIlE --------------------------
        try:
            with open('/home/ubuntu/meta-scrap-log.json', 'rb') as f:
                logd = f.read()
                pred = json.loads(logd)
                pred.append({
                    "dag" : "UN-INTERPOL-EN",
                    "date" : self.last_updated_string,
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

        with open(f'{self.Differencepath}/{self.date}_Difference_Output_UN_Entity.json', "w") as outfile:
            json.dump(new_profiles + updated_profiles, outfile)

        with open(f'{self.rmpath}/{self.date}_RemovedData_File_UN_Entity.json', "w") as outfile:
            json.dump(removed_profiles, outfile)

        if exists(self.lp_path):
            with open(f'{self.lp_path}',"a") as outfile:
                passing = f"{self.last_updated_string},{self.date}_input_UN_Entity.json,{self.date}_output_UN_Entity.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_Difference_Output_UN_Entity.json',{self.date}_RemovedData_File_UN_Entity.json'\n"
                outfile.write(passing)
        else:
            with open(f'{self.lp_path}',"a") as outfile:
                pass_first = "date,inputfile,outputfile,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
                passing = f"{self.last_updated_string},{self.date}_input_UN_Entity.json,{self.date}_output_UN_Entity.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_Difference_Output_UN_Entity.json',{self.date}_RemovedData_File_UN_Entity.json'\n"
                outfile.write(pass_first)
                outfile.write(passing)

    def parsing(self):
        try:
            for i in range(1,7):
                url = f"https://ws-public.interpol.int/notices/v1/un/entities?resultPerPage=20&page={i}"

                payload = {}
                headers = {
                    'accept': '*/*',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
                }

                res = requests.get(url, headers=headers, data=payload)
                body = json.loads(res.text)
                try:
                    data = body['_embedded']['notices']
                    self.input_list.extend(data)
                    last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                    for i in data:
                        try:
                            id = i['entity_id'].replace("/", "-")
                        except:
                            id = ""
                        try:
                            Final_url = f"https://ws-public.interpol.int/notices/v1/un/entities/{id}"

                            payload = {}
                            headers = {
                                'accept': '*/*',
                                'accept-encoding': 'gzip, deflate, br',
                                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
                            }

                            ress = requests.get(Final_url, headers=headers, data=payload)
                            bodyy = json.loads(ress.text)
                            try:
                                lname = bodyy['name']
                            except:
                                lname = ""
                            try:
                                name = bodyy['forename']
                            except:
                                name = ""
                            try:
                                fullname = name + lname
                            except:
                                fullname = ""

                            try:
                                birthdates = []
                                dateofbirth = bodyy['date_of_birth']
                            except:
                                dateofbirth = ""
                            aliasnames = []
                            try:
                                alias = bodyy['aliases']
                                for ii in alias:
                                    aliasname = ii['forename'] + ii['name']
                                    if aliasname != None or "":
                                        aliasnames.append(aliasname)
                            except:
                                aliasname = ""
                            try:
                                extra_name = bodyy['name_in_original_script']
                            except:
                                extra_name = ""
                            try:
                                extra_name2 = bodyy['forename_in_original_script']
                            except:
                                extra_name2 = ""
                            if extra_name2 != None or "":
                                aliasnames.append(extra_name2)
                            if extra_name != None or "":
                                aliasnames.append(extra_name)
                            try:
                                alias_birthdate = bodyy['aliases'][0]['date_of_birth']
                                alias_birthdate1 = bodyy['aliases'][1]['date_of_birth']
                            except:
                                alias_birthdate = ""
                                alias_birthdate1 = ""

                            try:
                                comment = bodyy['summary']
                            except:
                                comment = ""
                            try:
                                address = bodyy['adresses']
                            except:
                                address = ""

                            try:
                                country = bodyy['country_of_birth_id']
                                country = pycountry.countries.get(alpha_2=country).name
                            except:
                                country = ""
                            try:
                                associate = {}
                                associates = bodyy['associates']
                                for iii in associates:
                                    associates_name = iii['name']
                                    associates_number = iii['reference_number']
                                    associate['name'] = associates_name
                                    associate['reference_number'] = associates_number
                            except:
                                associates = ""
                                associate = ""
                            try:
                                sl_url = f"https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Entities#{id}"
                            except:
                                sl_url = ""
                            try:
                                while ("" in aliasnames):
                                    aliasnames.remove("")
                            except:
                                pass
                            item = {}
                            uid = fullname + str(dateofbirth) + "Interpol-United Nations Security Council Special Notices for Entities"
                            item['uid'] = hashlib.sha256(((uid).lower()).encode()).hexdigest()
                            item['name'] = fullname
                            item['alias_name'] = aliasnames
                            item['country'] = []
                            if country != None or country !="":
                                item['country'].append(country)

                            item['list_type'] = "Entity"
                            item['last_updated'] = last_updated_string
                            item['entity_details'] = {}
                            # item['entity_details']['date_of_birth'] = []
                            # if alias_birthdate != None or "":
                            #     item['entity_details']['date_of_birth'].append(alias_birthdate)
                            # if alias_birthdate1 != None or "":
                            #     item['entity_details']['date_of_birth'].append(alias_birthdate1)
                            # if dateofbirth != None or "":
                            #     item['entity_details']['date_of_birth'].append(dateofbirth)
                            # try:
                            #     while ("" in item['entity_details']['date_of_birth']):
                            #         item['entity_details']['date_of_birth'].remove("")
                            # except:
                            #     pass
                            item['entity_details']['associates'] = associate
                            item['entity_details']['nation'] = country
                            item['nns_status'] = False
                            item['address'] = []
                            add = {}
                            def_con = [coni.name for coni in list(pycountry.countries)]
                            # print(fullname)
                            if type(address) == list:
                                # print(1)
                                for ci in address:
                                    ci = ci.strip()
                                    if "(" in ci:
                                        con = ci[ci.find("(")+1:ci.find(")")]
                                        if con.strip().title() not in def_con:
                                            con = ""
                                        else:
                                            item['country'].append(con)
                                    else:
                                        con = ""
                                    item['address'].append({"complete_address":ci, "country":con})
    
                            else:
                                if address != None and address !="":
                                    # print(2)
                                    if country == "":
                                        add['complete_address'] = address
                                    else:
                                        add['complete_address'] = address + country
                                    add['country'] = country
                                else:
                                    # print(3)
                                    add['complete_address'] = ""
                                    add['country'] = ""
                                item['address'].append(add)
                            item["country"] = list(set(item['country']))
                            item["country"] = list(filter(("").__ne__, item["country"]))
                            item['comment'] = comment
                            item['sanction_list'] = {}
                            item['sanction_list']['sl_authority'] = "INTERPOL"
                            item['sanction_list']['sl_url'] = "https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Entities"
                            item['sanction_list']['sl_host_country'] = "United States of America"
                            item['sanction_list']['sl_type'] = "Sanctions"
                            item['sanction_list']['sl_source'] = "Interpol-United Nations Security Council Special Notices for Entities"
                            item['sanction_list']['sl_description'] = "The INTERPOL-United Nations Security Council Special Notice alerts global police to individuals and entities that are subject to sanctions imposed by the United Nations Security Council. The three most common sanctions are assets freeze, travel ban and arms embargo.."
                            item['sanction_list']['watch_list'] = "Global Watchlists"
                            item['list_id'] = "INT_T30094"
                            self.final_list.append(item)
                        except Exception as e:
                            print(e)
                except:
                    data = ""
            try:
                with open(f'{self.inputpath}/{self.date}_input_UN_Entity.json', "w") as infile:
                    json.dump(self.input_list, infile)
                with open(f'{self.outputpath}/{self.date}_output_UN_Entity.json', "w") as outfile:
                    json.dump(self.final_list, outfile)
            except Exception as e:
                print(e)

        except Exception as e:
            print(e)
temp = interpoleentity()
temp.parsing()
temp.compare()
temp.uploadToS3()