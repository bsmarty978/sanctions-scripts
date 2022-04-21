import requests
import json
import csv
import pandas as pd
import datetime
import re
import hashlib
import os
from os.path import exists
import boto3
# from datetime import datetime
import copy

import unidecode


last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
out_obj = []


class temp():
    try:
        datee = datetime.datetime.now().strftime("%Y_%m_%d")
        date = datetime.date.today()
        yesterday = date - datetime.timedelta(days=1)
        root = "/home/ubuntu/sanctions-scripts/AUSTRALIA/"
        inputpath = f'{root}InputFiles'
        outputpath = f'{root}OutputFiles'
        Differencepath = f'{root}DifferenceFiles'
        Removedpath = f'{root}RemovedFiles'
        lp_path = f'{root}logfile_au.csv'
        os.mkdir(inputpath)
        os.mkdir(outputpath)
        os.mkdir(Differencepath)
        os.mkdir(Removedpath)
    except FileExistsError:
        pass

    def uploadToS3(self):
        try:
            print("uploading files to s3")
            s3 = boto3.client('s3')
            s3.upload_file(f'{self.inputpath}/{self.date}_input_au.xls', "sams-scrapping-data",
                           f"AU/Original/{self.date}_input_au.xls")
            s3.upload_file(f'{self.outputpath}/{self.date}_output_au.json', "sams-scrapping-data",
                           f"AU/Parced/{self.date}_output_au.json")
            s3.upload_file(f'{self.Differencepath}/{self.date}_diffrance_au.json', "sams-scrapping-data",
                           f"AU/Diffrance/{self.date}_diffrance_au.json")
            s3.upload_file(f'{self.Removedpath}/{self.date}_removed_au.json', "sams-scrapping-data",
                           f"AU/Removed/{self.date}_removed_au.json")
            s3.upload_file(self.lp_path, "sams-scrapping-data",f"AU/{self.lp_path}")

            print("uploaded files to s3")
        except Exception as e:
            print("Error Uploading files over s3")
            print(e)

    # def compare(self):
    #     try:
    #         df_old_file = pd.read_excel(f'{self.inputpath}/{self.date}_input_au.xls',header=None)
    #         df_new_file = pd.read_excel(f'{self.inputpath}/{self.yesterday}_input_au.xls',header=None)
    #         header = df_new_file[0:1]
    #         df_old_file.drop([0, 0], 0, inplace=True)
    #         df_new_file.drop([0, 0], 0, inplace=True)
    #         df_lol = pd.concat([df_new_file, df_old_file])
    #         df_lol.columns = header.iloc[0]
    #         df_lol['Reference'] = df_lol['Reference'].astype(str)
    #         df_lol = df_lol.sort_values(by=["Reference"],ascending=True)
    #         df_lol1 = df_lol.drop_duplicates(subset=['Reference'], keep=False, inplace=False)
    #         df_lol1.to_excel(f'{self.Differencepath}/{self.date}_DifferenceFile_au.xls',index=False)
    #     except Exception as e:
    #         print(e)
    def download(self):
        try:
            url = "https://www.dfat.gov.au/sites/default/files/regulation8_consolidated.xls"
            res = requests.get(url)
            df = open(f"{self.inputpath}/{self.date}_input_au.xls", 'wb')
            df.write(res.content)
        except Exception as e:
            # print("Error : WHile Getting source DATA.")
            print(f"ERROR: {e}")
            raise ValueError("Error : Data Parsing Error.... fix it quickly ⚒️")


    def parse(self):
        data = []
        try:
            df = pd.read_excel(f'{self.inputpath}/{self.date}_input_au.xls')
            counter = 0
            uni_c  = 0
            for row in df.iterrows():
                item = {}
                uid = str(row[1]['Name of Individual or Entity']) + str(row[1]['Date of Birth']) + "Department of Foreign Affairs and Trade Sanctions List, Australia" + str(row[1]['Address'])
                item['uid'] = hashlib.sha256(((uid + "au-sanctions" + str(uni_c)).lower()).encode()).hexdigest()
                uni_c += 1
                item['name'] = row[1]['Name of Individual or Entity']
                item['name'] = unidecode.unidecode(item['name'])
                item['alias_name'] = []
                if row[1]['Name Type'] != "Primary Name":
                    data[counter - 1]['alias_name'].append(row[1]['Name of Individual or Entity'])
                    # data[counter - 1]['alias_name'].append(unidecode.unidecode(row[1]['Name of Individual or Entity']))
                else:
                    item['country'] = []
                    country = str(row[1]['Citizenship']).replace('"NaN"',"").replace("nan","")
                    item['country'].append(country)
                    item['list_type'] = row[1]['Type']
                    # item['list_type'] = "Entity"
                    item['last_updated'] = last_updated_string
                    list_type = row[1]['Type']
                    if list_type == "Individual":
                        item[f'individual_details'] = {}
                        item[f'individual_details']['date_of_birth'] = []
                        date_of_birth = re.findall('(\d{4})', str(row[1]['Date of Birth']))
                        item[f'individual_details']['date_of_birth'].append(date_of_birth)
                    # if len(date_of_birth) == 4:
                    #     for i in range(int(date_of_birth[0]), int(date_of_birth[1])):
                    #         item['individual_details']['date_of_birth'].append(i)
                    #     item['individual_details']['date_of_birth'].append(date_of_birth[1])
                    #     for j in range(int(date_of_birth[-2]), int(date_of_birth[-1])):
                    #         item['individual_details']['date_of_birth'].append(j)
                    #     item['individual_details']['date_of_birth'].append(date_of_birth[-1])
                    # elif len(date_of_birth) == 1:
                    #     item['individual_details']['date_of_birth'].append(''.join(date_of_birth))
                    # elif len(date_of_birth) == 2:
                    #     for h in range(int(date_of_birth[0]), int(date_of_birth[1])):
                    #         item['individual_details']['date_of_birth'].append(h)
                    #     item['individual_details']['date_of_birth'].append(date_of_birth[1])
                    # elif len(date_of_birth) == 3:
                    #     for g in range(int(date_of_birth[0]), int(date_of_birth[1])):
                    #         item['individual_details']['date_of_birth'].append(g)
                    #     item['individual_details']['date_of_birth'].append(date_of_birth[1])
                    #     item['individual_details']['date_of_birth'].append(date_of_birth[-1])
                        item[f'individual_details']['place_of_birth'] = str(row[1]['Place of Birth']).replace('"NaN"',"").replace("nan","")
                        item[f'individual_details']['citizenship'] = str(row[1]['Citizenship']).replace('"NaN"',"").replace("nan","")
                        item['designation'] = str(row[1]['Additional Information']).replace('"NaN"',"").replace("nan","")
                        item['address'] = []
                        add = {}
                        add['country'] = str(row[1]['Citizenship']).replace('"NaN"',"").replace("nan","")
                        add['complete_address'] = str(row[1]['Address']).replace('"NaN"',"").replace("nan","")
                        item['address'].append(add)
                        item['documents'] = {}
                        item['comment'] = ""
                        item['sanction_list'] = {}
                        item['sanction_list']['sl_authority'] = "The Department of Foreign Affairs and Trade, Australia"
                        item['sanction_list']['sl_url'] = "https://www.dfat.gov.au/"
                        item['sanction_list']['sl_host_country'] = "Australia"
                        item['sanction_list']['sl_type'] = "Sanctions"
                        item['sanction_list']['sl_source'] = "Department of Foreign Affairs and Trade Sanctions List, Australia"
                        item['sanction_list']['sl_description'] = "The Consolidated List is a list of all persons and entities who are subject to targeted financial sanctions under Australian sanctions law. Those listed may be Australian citizens, foreign nationals, or residents in Australia or overseas."
                        item['sanction_list']['watch_list'] = "APAC Watchlists"
                        item['list_id'] = "AUS_S10009"
                        data.append(item)
                        counter = counter + 1
                    else:
                        item['entity_details'] = {}
                        item['nns_status'] = False
                        item['address'] = []
                        add = {}
                        add['country'] = str(row[1]['Citizenship']).replace('"NaN"', "").replace("nan", "")
                        add['complete_address'] = str(row[1]['Address']).replace('"NaN"', "").replace("nan", "")
                        item['designation'] = str(row[1]['Additional Information']).replace('"NaN"', "").replace("nan","")
                        item['address'].append(add)
                        item['documents'] = {}
                        item['comment'] = ""
                        item['sanction_list'] = {}
                        item['sanction_list']['sl_authority'] = "The Department of Foreign Affairs and Trade, Australia"
                        item['sanction_list']['sl_url'] = "https://www.dfat.gov.au/"
                        item['sanction_list']['sl_host_country'] = "Australia"
                        item['sanction_list']['sl_type'] = "Sanctions"
                        item['sanction_list']['sl_source'] = "Department of Foreign Affairs and Trade Sanctions List, Australia"
                        item['sanction_list']['sl_description'] = "The Consolidated List is a list of all persons and entities who are subject to targeted financial sanctions under Australian sanctions law. Those listed may be Australian citizens, foreign nationals, or residents in Australia or overseas."
                        item['sanction_list']['watch_list'] = "APAC Watchlists"
                        item['list_id'] = "AUS_S10009"
                        data.append(item)
                        counter = counter+1
            try:
                # ab = json.dumps(data)
                f = open(f"{self.outputpath}/{self.date}_output_au.json", "w",encoding='utf-8')
                # f.write(ab)1
                # json.dump(data,f,ensure_ascii=False)
                json.dump(data,f)
                global out_obj
                out_obj = copy.deepcopy(data)
                f.close()
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)

    def CompareDocument(self):
        print("Called _commpare")
        try:
            with open(f'{self.outputpath}/{self.yesterday}_output_au.json', 'rb') as f:
                data = f.read()
        except:
            print("---------------------Alert--------------------------")
            print(f"There is not any old file for date: {self.yesterday.ctime()}")
            print("----------------------------------------------------")
            data = "No DATA"
        old_list = []
        global out_obj
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
        print("------------------------LOG-DATA---------------------------")
        print(f"total New Profiles Detected:     {len(new_profiles)}")
        print(f"total Updated Profiles Detected: {len(updated_profiles)}")
        print(f"total Removed Profiles Detected: {len(removed_profiles)}")
        print("-----------------------------------------------------------")
        if len(new_list)==0:
            removed_profiles = []
            raise ValueError("Data Parsing Error Fix it Quickly ⚒️")
        #NOTE: ---------- META LOg FIlE --------------------------
        try:
            with open('/home/ubuntu/meta-scrap-log.json', 'rb') as f:
                logd = f.read()
                pred = json.loads(logd)
                pred.append({
                    "dag" : "AUSTRALIA",
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
            with open(f'{self.Differencepath}/{self.date}_diffrance_au.json', "w",encoding='utf-8') as outfile:
                json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)
        except FileNotFoundError:
            os.mkdir(self.Differencepath)
            with open(f'{self.Differencepath}/{self.date}_diffrance_au.json', "w",encoding='utf-8') as outfile:
                json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)

        try:
            with open(f'{self.Removedpath}/{self.date}_removed_au.json', "w",encoding='utf-8') as outfile:
                json.dump(removed_profiles, outfile,ensure_ascii=False)
        except FileNotFoundError:
            os.mkdir(self.Removedpath)
            with open(f'{self.Removedpath}/{self.date}_removed_au.json', "w",encoding='utf-8') as outfile:
                json.dump(removed_profiles, outfile,ensure_ascii=False)

        if exists(self.lp_path):
            with open(f'{self.lp_path}',"a") as outfile:
                passing = f"{last_updated_string},{self.date}_input_au.csv,{self.date}_output_au.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_diffrance_au.json',{self.date}_removed_au.json'\n"
                outfile.write(passing)
        else:
            with open(f'{self.lp_path}',"a") as outfile:
                pass_first = "date,inputfile,outputfile,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
                passing = f"{last_updated_string},{self.date}_input_au.csv,{self.date}_output_au.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_diffrance_au.json',{self.date}_removed_au.json'\n"
                outfile.write(pass_first)
                outfile.write(passing)

run = temp()
run.download()
run.parse()
run.CompareDocument()
run.uploadToS3()