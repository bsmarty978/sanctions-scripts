import hashlib
import json
import pandas as pd
import csv
from datetime import datetime
import requests
import os
import datetime
from os import listdir
from glob import glob
import time as t
import boto3
import os
from os.path import exists
import unidecode
import copy
import traceback

last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
out_obj = []
class conlist():
    try:
        datee = datetime.datetime.now().strftime("%Y_%m_%d")
        date = datetime.date.today()
        yesterday = date - datetime.timedelta(days=1)
        root = ""
        inputpath = f'{root}InputFiles'
        outputpath = f'{root}OutputFiles'
        Differencepath = f'{root}DifferenceFiles'
        Removedpath = f'{root}RemovedFiles'
        lp_path = f'{root}logfile_uk.csv'
        os.mkdir(inputpath)
        os.mkdir(outputpath)
        os.mkdir(Differencepath)
        os.mkdir(Removedpath)
    except Exception as e:
        print(e)
    def uploadToS3(self):
        try:
            print("uploading files to s3")
            s3 = boto3.client('s3')
            s3.upload_file(f'{self.inputpath}/{self.date}_input_uk.csv', "sams-scrapping-data", f"UK/Original/{self.date}_input_uk.csv")
            s3.upload_file(f'{self.outputpath}/{self.date}_output_uk.json', "sams-scrapping-data", f"UK/Parced/{self.date}_output_uk.json")
            s3.upload_file(f'{self.Differencepath}/{self.date}_DifferenceFile_uk.csv', "sams-scrapping-data",
                           f"UK/Diffrance/{self.date}_DifferenceFile_uk.csv")
            print("uploaded files to s3")
        except Exception as e:
           print(e)

    def compare(self):
        try:
            df_old_file = pd.read_csv(f'{self.inputpath}/{self.date}_input_uk.csv',encoding='unicode_escape',skiprows=1,header=None)
            df_new_file = pd.read_csv(f'{self.inputpath}/{self.yesterday}_input_uk.csv',encoding='unicode_escape',skiprows=1,header=None)
            header = df_new_file[0:1]
            df_old_file.drop([0, 1], 0, inplace=True)
            df_new_file.drop([0, 1], 0, inplace=True)
            df_lol = pd.concat([df_new_file, df_old_file])
            df_lol.columns = header.iloc[0]
            df_lol = df_lol.sort_values("Group ID")
            df_lol1 = df_lol.drop_duplicates(subset=['Group ID'], keep=False, inplace=False)
            df_lol1.to_csv(f'{self.Differencepath}/{self.date}_DifferenceFile_uk.csv')
        except Exception as e:
            pass

    def parsing(self):
        list = []
        templ = []
        try:
            try:
                # f =  open(f'{self.Differencepath}/{self.date}_DifferenceFile_uk.csv',newline='')
                # f = open(f'{self.inputpath}/{self.date}_input_uk.csv', newline='',encoding='windows-1252')
                f = open(f'{self.inputpath}/{self.date}_input_uk.csv', "r")
            except:
                print("Inpuut File Not Found")
            reader = csv.reader(f)
            counter = 0
            for i in reader:
                if "Last Updated" in i  or  "Name 6" in i:
                    continue
                else:
                    try:
                        item = {}
                        name = []
                        name1 = i[1]
                        name2 = i[2]
                        name3 = i[3]
                        name4 = i[4]
                        name5 = i[5]
                        name6 = i[0]
                        name.append(name1.replace('‘',"'").strip())
                        name.append(name2.replace('‘',"'").strip())
                        name.append(name3.replace('‘',"'").strip())
                        name.append(name4.replace('‘',"'").strip())
                        name.append(name5.replace('‘',"'").strip())
                        name.append(name6.replace('‘',"'").strip())
                        item['groupid'] = i[28]
                        uid = ''.join(name) + i[9] + "HM's Treasury Financial Sanctions List -United Kingdom"
                        item['uid'] =  hashlib.sha256(((uid).lower()).encode()).hexdigest()
                        item['name'] = ' '.join(name).strip()
                        item['name'] = unidecode.unidecode(item['name'])
                        norm = ""
                        for n in item['name'].split(" "):
                            if n:
                                norm = norm + " " + n.strip()
                        item['name'] = norm.strip('"').strip()
                        item['list_id'] = "UK_S10008"
                        item['country'] = []
                        item['country'].append(i[21])
                        item['alias_name'] = []
                        if item['groupid'] in templ:
                            for j in list:
                                if j['groupid'] == item['groupid']:
                                    j['alias_name'].append(' '.join(name).strip())
                            # new_alis = []
                            # for ali in item['alias_name']:
                            #     alinorm = ""
                            #     for n in ali.split(" "):
                            #         if n:
                            #             alinorm = alinorm + " " + n.strip()
                            #     new_alis.append(alinorm.strip())
                            # item["alias_name"] = new_alis
                            
                        else:
                            templ.append(item['groupid'])
                            item['list_type'] = i[23]
                            item['last_updated'] = last_updated_string
                            list_type = i[23]
                            if list_type == "Individual":
                                item[f'individual_details'] = {}
                                item[f'individual_details']['date_of_birth'] = []
                                item[f'individual_details']['date_of_birth'].append(i[7])
                                item[f'individual_details']['town_of_birth'] = i[8]
                                item[f'individual_details']['country_of_Birth'] = i[9]
                                item[f'individual_details']['Nationality'] = i[10]
                                item['documents'] = {}
                                item['documents']['passport_details'] = i[11]
                                item['documents']['ni_number'] = i[12]
                                item['address'] = []
                                full_address = []
                                Address1 = i[14]
                                Address2 = i[15]
                                Address3 = i[16]
                                Address4 = i[17]
                                Address5 = i[18]
                                Address6 = i[19]
                                full_address.append(Address1)
                                full_address.append(Address2)
                                full_address.append(Address3)
                                full_address.append(Address4)
                                full_address.append(Address5)
                                full_address.append(Address6)
                                address = {}
                                address['complete_address'] = ''.join(full_address)
                                address['zipCode'] = i[20]
                                address['country'] = i[21]
                                item['address'].append(address)
                                item['sanction_Details'] = {}
                                item['sanction_Details']['regime'] = i[25]
                                item['sanction_Details']['listed_on'] = i[27]
                                item['sanction_list'] = {}
                                item['sanction_list']['sl_authority'] = "The Office of Financial Sanctions Implementation (OFSI) of the HM Treasury, The United Kingdom Government"
                                item['sanction_list']['sl_url'] = "https://ofsistorage.blob.core.windows.net/publishlive/ConList.csv"
                                item['sanction_list']['sl_host_country'] = "United Kingdom"
                                item['sanction_list']['sl_type'] = "Sanctions"
                                item['sanction_list']['watch_list'] = "European Watchlists"
                                # item['sanction_list']['sl_source'] = "HM's Treasury Financial Sanctions List -United Kingdom"
                                item['sanction_list']['sl_source'] = "HM Treasury List"
                                item['sanction_list']['sl_description'] = "HMT sanctions list is a list of entities and individuals subjected to certain financial restrictions as part of the United Kingdom's government's domestic counter-terrorism regime policy. Also, it includes individuals prohibited by the European Union and/or the United Nations."
                                list.append(item)
                            else:
                                item['entity_details'] = {}
                                item['nns_status'] = False
                                item['address'] = []
                                full_address = []
                                Address1 = i[14]
                                Address2 = i[15]
                                Address3 = i[16]
                                Address4 = i[17]
                                Address5 = i[18]
                                Address6 = i[19]
                                full_address.append(Address1)
                                full_address.append(Address2)
                                full_address.append(Address3)
                                full_address.append(Address4)
                                full_address.append(Address5)
                                full_address.append(Address6)
                                address = {}
                                address['complete_address'] = ''.join(full_address)
                                address['zipCode'] = i[20]
                                address['country'] = i[21]
                                item['address'].append(address)
                                item['sanction_Details'] = {}
                                item['sanction_Details']['regime'] = i[25]
                                item['sanction_Details']['listed_on'] = i[27]
                                item['documents'] = {}
                                item['comment'] = ""
                                item['sanction_list'] = {}
                                item['sanction_list']['sl_authority'] = "The Office of Financial Sanctions Implementation (OFSI) of the HM Treasury, The United Kingdom Government"
                                item['sanction_list']['sl_url'] = "https://ofsistorage.blob.core.windows.net/publishlive/ConList.csv"
                                item['sanction_list']['sl_host_country'] = "United Kingdom"
                                item['sanction_list']['sl_type'] = "Sanctions"
                                item['sanction_list']['watch_list'] = "European Watchlists"
                                # item['sanction_list']['sl_source'] = "HM's Treasury Financial Sanctions List -United Kingdom"
                                item['sanction_list']['sl_source'] = "HM Treasury List"
                                item['sanction_list']['sl_description'] = "HMT sanctions list is a list of entities and individuals subjected to certain financial restrictions as part of the United Kingdom's government's domestic counter-terrorism regime policy. Also, it includes individuals prohibited by the European Union and/or the United Nations."
                                list.append(item)
                    
                    except Exception as e:
                        print(e)
            try:
                for ii in list:
                    # new_alis = []
                    # for ali in ii['alias_name']:
                    #     alinorm = ""
                    #     for n in ali.split(" "):
                    #         if n:
                    #             alinorm = norm + " " + n.strip()
                    #     new_alis.append(alinorm)
                    # ii["alias_name"] = new_alis
                    ii.pop("groupid")
                # ab = json.dumps(list)
                f = open(f"{self.outputpath}/{self.date}_output_uk.json", "w")
                # f.write(ab)
                global out_obj
                out_obj = copy.deepcopy(list)
                json.dump(list,f,indent=2)
                f.close()
            except Exception as e:
                print(e)
        
            # t.sleep(5)
            # self.CompareDocument()
        except Exception as e:
            print(e)
            print("Exception : " , traceback.print_exc())

    def download(self):
        try:
            # url = "https://ofsistorage.blob.core.windows.net/publishlive/ConList.csv"
            url = "https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv"
            res = requests.get(url)
            df = open(f"{self.inputpath}/{self.date}_input_uk.csv", 'wb')
            df.write(res.content)
            t.sleep(5)
            self.parsing()
        except Exception as e:
            print(e)

    def CompareDocument(self):
        print("Called _commpare")
        try:
            with open(f'{self.outputpath}/{self.yesterday}_output_uk.json', 'rb') as f:
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

        print("------------------------LOG-DATA---------------------------")
        print(f"total New Profiles Detected:     {len(new_profiles)}")
        print(f"total Updated Profiles Detected: {len(updated_profiles)}")
        print(f"total Removed Profiles Detected: {len(removed_profiles)}")
        print("-----------------------------------------------------------")

        try:
            with open(f'{self.Differencepath}/{self.date}_diffrance_uk.json', "w",encoding='utf-8') as outfile:
                json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)
        except FileNotFoundError:
            os.mkdir(self.Differencepath)
            with open(f'{self.Differencepath}/{self.date}_diffrance_uk.json', "w",encoding='utf-8') as outfile:
                json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)

        try:
            with open(f'{self.Removedpath}/{self.date}_removed_uk.json', "w",encoding='utf-8') as outfile:
                json.dump(removed_profiles, outfile,ensure_ascii=False)
        except FileNotFoundError:
            os.mkdir(self.Removedpath)
            with open(f'{self.Removedpath}/{self.date}_removed_uk.json', "w",encoding='utf-8') as outfile:
                json.dump(removed_profiles, outfile,ensure_ascii=False)

        if exists(self.lp_path):
            with open(f'{self.lp_path}',"a") as outfile:
                passing = f"{last_updated_string},{self.date}_input_uk.csv,{self.date}_output_uk.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_diffrance_uk.json',{self.date}_removed_uk.json'\n"
                outfile.write(passing)
        else:
            with open(f'{self.lp_path}',"a") as outfile:
                pass_first = "date,inputfile,outputfile,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
                passing = f"{last_updated_string},{self.date}_input_uk.csv,{self.date}_output_uk.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_diffrance_uk.json',{self.date}_removed_uk.json'\n"
                outfile.write(pass_first)
                outfile.write(passing)

if __name__ == '__main__':
    temp = conlist()
    temp.download()
    # temp.parsing()
    # temp.uploadToS3()
