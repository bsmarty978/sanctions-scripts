import requests
import json
import csv
import pandas as pd
import datetime
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


class temp():
    try:
        final_list = []
        date = datetime.date.today()
        yesterday = date - datetime.timedelta(days=1)
        inputpath = f'InputFiles'
        outputpath = f'OutputFiles'
        Differencepath = f'DifferenceFiles'
        os.mkdir(inputpath)
        os.mkdir(outputpath)
        os.mkdir(Differencepath)
    except:
        pass

    def uploadToS3(self):
        try:
            print("uploading files to s3")
            s3 = boto3.client('s3')
            s3.upload_file(f'{self.inputpath}/{self.date}_input_cbr.ru.xls', "sams-scrapping-data",
                           f"RU/Original/{self.date}_input_cbr.ru.xls")
            s3.upload_file(f'{self.outputpath}/{self.date}_output_cbr.ru.json', "sams-scrapping-data",
                           f"RU/Parced/{self.date}_output_cbr.ru.json")
            s3.upload_file(f'{self.Differencepath}/{self.date}_DifferenceFile_cbr.ru.xls', "sams-scrapping-data",
                           f"RU/Diffrance/{self.date}_DifferenceFile_cbr.ru.xls")
            print("uploaded files to s3")
        except Exception as e:
            print(e)

    def compare(self):
        try:
            with open(f'{self.outputpath}/{self.yesterday}_output_cbr.ru.json', 'rb') as f:
                data = f.read()
        except:
            # print("---------------------Alert--------------------------")
            print(f"There is not any old file for date: {self.yesterday.ctime()}")
            # print("----------------------------------------------------")
            data = "No DATA"
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

            with open(f'{self.outputpath}/{self.date}_Difference_output_cbr.ru.json',
                      "w") as outfile:
                json.dump(new_profiles + updated_profiles, outfile)

            with open(f'{self.date}_RemovedData_File_cbr.ru.json', "w") as outfile:
                json.dump(removed_profiles, outfile)

            # with open(f'ofac-sdn-logfile.csv', "a") as outfile:
            #     passing = f"{today_date.ctime()},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles) + len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            #     outfile.write(passing)
    def download(self):
        try:
            url = "https://cbr.ru/Queries/UniDbQuery/DownloadExcel/123126?FromDate=04/05/2022&ToDate=04/05/2022&posted=False"
            res = requests.get(url)
            df = open(f"{self.inputpath}/{self.date}_input_cbr.ru.xls", 'wb')
            df.write(res.content)
        except Exception as e:
            print(e)

    def parse(self):
        try:
            try:
                df = pd.read_excel(f'{self.Differencepath}/{self.date}_DifferenceFile_au.xls')
            except:
                df = pd.read_excel(f'{self.inputpath}/{self.date}_input_cbr.ru.xls')
            counter = 0
            for row in df.iterrows():
                item = {}
                item['uid'] = hashlib.sha256(((row[1]['Название']).lower()).encode()).hexdigest()
                lol = row[1]['Название']
                # try:
                #     name = GoogleTranslator(source='auto', target='en').translate(lol)
                # except:
                #     try:
                #         name = GoogleTranslator(source='auto', target='en').translate(lol)
                #     except:
                #         print(f">>>Translartion Bug : {lol}")
                #         name = lol
                        # name_eng = Translator()
                name = lol
                #
                # try:
                #     name = name_eng.translate(lol,lang_tgt='en')
                #     print(name.text)
                # except Exception as e:
                #     name = row[1]['Название']
                #     print(e)
                # translation = translator.translate("Good Morning!")
                item['name'] = name
                item['alias_name'] = []
                item['alias_name'].append(row[1]['Название'])
                item['country'] = []
                item['country'].append("Russia")
                item['list_type'] = "Entity"
                item['last_updated'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                item['entity_details'] = {}
                item['entity_details']['Website'] = str(row[1]['Сайт']).replace("NAN","").replace("nan","")
                item['entity_details']['Phone_Number'] = str(row[1]['ИНН']).replace("NAN","").replace("nan","")
                item['sanction_details'] = {}
                item['sanction_details']['last_updated_Date'] = str(row[1]['Дата'])
                item['nns_status'] = False
                item['address'] = []
                add = {}
                add['country'] = "Russia"
                add['complete_address'] = str(row[1]['Адрес']).replace("NAN","").replace("nan","")
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
                self.final_list.append(item)
                counter = counter+1
            try:
                ab = json.dumps(self.final_list)
                f = open(f"{self.outputpath}/{self.date}_output_cbr.ru.json", "a")
                f.write(ab)
                f.close()
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)



run = temp()
run.download()
# run.compare()
run.parse()
# run.uploadToS3()