import traceback
import requests
import json
import csv
import pandas as pd
from datetime import datetime,date,timedelta
import re
import hashlib
import os
import boto3
import copy
# from translate import Translator
# from googletrans import Translator
# from google_trans_new import google_translator
# import google_trans_new as gtn
# from datetime import datetime
from deep_translator import GoogleTranslator
from os.path import exists


class temp():
    try:
        #NOTE: Object for output json file
        out_list = []
        total_profile_available = 0

        #NOTE: Filename according to the date :
        today_date  = date.today()
        yesterday = today_date - timedelta(days = 1)
        last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        dag_name = "cbr-ru"

        input_filename  = f'{dag_name}-input-{today_date.day}-{today_date.month}-{today_date.year}.xls'
        output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
        diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
        removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
        old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
        lp_name = f'{dag_name}-logfile.csv'
        #NOTE: Paths of directories
        root = "/home/ubuntu/sanctions-scripts/CBR-RU/"
        # root = ""
        ip_path = f"{root}inputfiles"
        op_path = f"{root}outputfiles"
        dp_path = f"{root}diffrancefiles"
        rm_path = f"{root}removedfiles"
        lp_path = f"{root}{dag_name}-logfile.csv"

    except:
        pass

    def UploadfilestTos3(self):
        try:
            print("uploading files to s3")
            s3 = boto3.client('s3')
            s3.upload_file(f'{self.ip_path}/{self.input_filename}',"sams-scrapping-data",f"{self.dag_name}/original/{self.input_filename}")
            s3.upload_file(f'{self.op_path}/{self.output_filename}',"sams-scrapping-data",f"{self.dag_name}/parced/{self.output_filename}")
            s3.upload_file(f'{self.dp_path}/{self.diffrance_filename}',"sams-scrapping-data",f"{self.dag_name}/diffrance/{self.diffrance_filename}")
            s3.upload_file(f'{self.rm_path}/{self.removed_filename}',"sams-scrapping-data",f"{self.dag_name}/removed/{self.removed_filename}")
            s3.upload_file(f'{self.lp_path}',"sams-scrapping-data",f"{self.dag_name}/{self.lp_name}")
            print("uploaded files to s3")      
        except Exception as e:
            print("------------------🔴ALERT🔴------------------------")
            print("Can not upload files to s3")
            print("Exception : " , e)
            print("----------------------------------------------------")


    def CompareDocument(self):
        try:
            with open(f'{self.op_path}/{self.old_output_filename}', 'rb') as f:
                data = f.read()
        except:
            print("---------------------Alert--------------------------")
            print(f"There is not any old file for date: {self.yesterday.ctime()}")
            print("----------------------------------------------------")
            data = "No DATA"
        old_list = []
        if data != "No DATA":   
            old_list = json.loads(data)
        new_list = copy.deepcopy(self.out_list)

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

        try:
            with open(f'{self.dp_path}/{self.diffrance_filename}', "w",encoding='utf-8') as outfile:
                json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)
        except FileNotFoundError:
            os.mkdir(self.dp_path)
            with open(f'{self.dp_path}/{self.diffrance_filename}', "w",encoding='utf-8') as outfile:
                json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)

        try:
            with open(f'{self.rm_path}/{self.removed_filename}', "w",encoding='utf-8') as outfile:
                json.dump(removed_profiles, outfile,ensure_ascii=False)
        except FileNotFoundError:
            os.mkdir(self.rm_path)
            with open(f'{self.rm_path}/{self.removed_filename}', "w",encoding='utf-8') as outfile:
                json.dump(removed_profiles, outfile,ensure_ascii=False)

        if exists(self.lp_path):
            with open(f'{self.lp_path}',"a") as outfile:
                passing = f"{self.last_updated_string},{self.input_filename},{self.output_filename},{self.total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.diffrance_filename},{self.removed_filename}\n"
                outfile.write(passing)
        else:
            with open(f'{self.lp_path}',"a") as outfile:
                pass_first = "date,inputfile,outputfile,total_profile_availabe,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
                passing = f"{self.last_updated_string},{self.input_filename},{self.output_filename},{self.total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.diffrance_filename},{self.removed_filename}\n"
                outfile.write(pass_first)
                outfile.write(passing)


    def download(self):
        try:
            url = "https://cbr.ru/Queries/UniDbQuery/DownloadExcel/123126?FromDate=04/05/2022&ToDate=04/05/2022&posted=False"
            res = requests.get(url)
            # df = open(f"{self.inputpath}/{self.date}_input_cbr.ru.xls", 'wb')
            # df.write(res.content)
            try:
                with open(f'{self.ip_path}/{self.input_filename}', "wb") as infile:
                    infile.write(res.content)
            except FileNotFoundError:
                os.mkdir(self.ip_path)
                with open(f'{self.ip_path}/{self.input_filename}', "wb") as infile:
                    infile.write(res.content)

        except Exception as e:
            print(e)

    # -*- coding: utf-8 -*-
    def isEnglish(self,s):
        try:
            s.encode(encoding='utf-8').decode('ascii')
        except UnicodeDecodeError:
            return False
        else:
            return True


    def langtranslation(self,to_translate):
        try:
            translated = GoogleTranslator(source='auto', target='en').translate(to_translate)
        except:
            try:
                translated = GoogleTranslator(source='auto', target='en').translate(to_translate)
            except:
                print(f">>>Translartion Bug : {to_translate}")
                translated = to_translate  
        return translated

    def parse(self):
        try:
            df = pd.read_excel(f'{self.ip_path}/{self.input_filename}')
            counter = 0
            for row in df.iterrows():
                item = {}
                item['uid'] = hashlib.sha256(((row[1]['Название'] + str(row[1]['Адрес']).replace("NAN","").replace("nan","")).lower()).encode()).hexdigest()
                
                #NOTE: NAME TRANSALATION
                lol = row[1]['Название']
                if not self.isEnglish(lol):
                    name = self.langtranslation(lol)
                else:
                    name = lol

                print(f'--->{name}')
                #NOTE: ADDRESS TRANSLATION
                address = str(row[1]['Адрес']).replace("NAN","").replace("nan","")
                if not self.isEnglish(address) and address.strip()!="":
                    address =  self.langtranslation(address)
                
                #
                # try:
                #     name = name_eng.translate(lol,lang_tgt='en')
                #     print(name.text)
                # except Exception as e:
                #     name = row[1]['Название']
                #     print(e)
                # translation = translator.translate("Good Morning!")
                item['name'] = name.replace(",","")
                item['alias_name'] = []
                item['alias_name'].append(row[1]['Название'])
                item['country'] = []
                item['country'].append("Russia")
                item['list_type'] = "Entity"
                item['last_updated'] = self.last_updated_string
                item['entity_details'] = {}
                item['entity_details']['Website'] = str(row[1]['Сайт']).replace("NAN","").replace("nan","")
                item['entity_details']['Phone_Number'] = str(row[1]['ИНН']).replace("NAN","").replace("nan","")
                item['sanction_details'] = {}
                item['sanction_details']['last_updated_Date'] = str(row[1]['Дата'])
                item['nns_status'] = False
                item['address'] = []
                add = {}
                add['country'] = "Russia"
                add['complete_address'] = address
                item['address'].append(add)
                item['documents'] = {}
                item['comment'] = ""
                item['sanction_list'] = {}
                item['sanction_list']['sl_authority'] = "Central Bank of the Russian Federation (Bank of Russia), Russia"
                item['sanction_list']['sl_url'] = "https://cbr.ru/inside/warning-list/"
                item['sanction_list']['sl_host_country'] = "Russia"
                item['sanction_list']['sl_type'] = "Sanctions"
                item['sanction_list']['sl_source'] = "Central Bank of Russia Special Economic Measures List, Russia"
                item['sanction_list']['sl_description'] = "List of companies with identified signs of illegal activity in the financial market by Central Bank of the Russian Federation (Bank of Russia), Russia."
                item['sanction_list']['watch_list'] = "APAC Watchlists"
                item['list_id'] = "RUS_E20240"
                self.out_list.append(item)
                counter = counter+1
            self.total_profile_available = len(self.out_list)
            print(f"Total profile available : {self.total_profile_available}")
            try:
                with open(f'{self.op_path}/{self.output_filename}', "w",encoding='utf-8') as outfile:
                    json.dump(self.out_list, outfile,ensure_ascii=False,indent=2)
            except FileNotFoundError:
                os.mkdir(self.op_path)
                with open(f'{self.op_path}/{self.output_filename}', "w",encoding='utf-8') as outfile:
                        json.dump(self.out_list, outfile,ensure_ascii=False,indent=2)  
        except Exception as e:
            print(traceback.print_exc())



run = temp()
run.download()
run.parse()
run.CompareDocument()
run.UploadfilestTos3()