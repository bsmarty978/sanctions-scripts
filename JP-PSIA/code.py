from deep_translator import GoogleTranslator
import requests
import json
from scrapy.http import HtmlResponse
import datetime
import os
import time as t
import boto3
import copy
import hashlib


class Data():
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
            s3.upload_file(f'{self.outputpath}/{self.date}_output_sng-771.ch.json', "sams-scrapping-data",
                           f"USA/Narcotics/Parced/{self.date}_output_sng-771.ch.json")
            s3.upload_file(f'{self.Differencepath}/{self.date}_Difference_output_sng-771.ch.json', "sams-scrapping-data",
                           f"USA/Narcotics/Diffrance/{self.date}_Difference_output_sng-771.ch.json")
            print("uploaded files to s3")
        except Exception as e:
            print(e)

    def compare(self):
        try:
            with open(f'{self.outputpath}/{self.yesterday}_output_sng-771.ch.json', 'rb') as f:
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

            with open(f'{self.outputpath}/{self.date}_Difference_output_sng-771.ch.json', "w") as outfile:
                json.dump(new_profiles + updated_profiles, outfile)

            with open(f'{self.date}_RemovedData_File_sng-771.ch', "w") as outfile:
                json.dump(removed_profiles, outfile)

            # with open(f'ofac-sdn-logfile.csv', "a") as outfile:
            #     passing = f"{today_date.ctime()},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles) + len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            #     outfile.write(passing)

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

    def parsing(self):
        try:
            url = "https://www.moj.go.jp/psia/ITH/organizations/index.html#test1"
            # payload = {}
            # headers = {
            #     'Cookie': 'PHPSESSID=af56ab0690cba426b0cc6c15027b89ab; TS01b6fa6b=01885b66e8083558d8eaf949ade0cfcf75a46c3a69088d1f4d4449f9cc99a84c036c294521c0b623e42f2fa621ca98fce3e457dadce5be3b1125df83ae024650a0806c8139'
            # }

            res = requests.get(url)
            response = HtmlResponse(url="example.com", body=res.content)
            # print(response)

            divs = response.xpath('//*[@class="tab_table "]//tbody/tr')
            for div in divs:
                try:
                    alias = []
                    name1 = div.xpath('./td[1]//text()').extract_first(default="").strip()
                    alias.append(name1)
                    name2 = self.langtranslation(name1).replace('"','')
                    if '(' in name2:
                        name = name2.split('(')[0]
                        name0 = name2.split('(')[1].replace(')','')
                        alias.append(name)
                        alias.append(name0)
                    else:
                        name = name2
                        alias.append(name2)
                except:
                    name = ""
                    alias = []
                # print(name)

                try:
                    cts = []
                    country1 = div.xpath('./td[3]//text()').get()
                    country = self.langtranslation(country1)
                    if '(' and ')' in country:
                        ctr1 = country.split('(')[0].strip()
                        ctr2 = country.split(')')[1].strip()
                        ctr = ctr1+ctr2
                        ctr0 = ctr.split(',')
                        cts.append(ctr0)
                    else:
                        country0 = country.split(',')
                        cts.append(country0)
                except Exception as e:
                    print(e)
                    cts = []

                item = {}
                last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                item = {}
                item['uid'] = hashlib.sha256(((name).lower()).encode()).hexdigest()
                item['name'] = name
                item['alias_name'] = alias
                item['country'] = []
                item['list_type'] = "Entity"
                item['last_updated'] = last_updated_string
                item['entity_details'] = {}
                # item['entity_details']['website'] = []
                item['nns_status'] = False
                item['address'] = []
                for ctt in cts:
                    for ct in ctt:
                        print(ct)
                        add = {}
                        add['complete_address'] = ""
                        add['country'] = ct
                        item['address'].append(add)
                item['sanction_details'] = {}
                item['documents'] = {}
                item['comment'] = []
                item['sanction_list'] = {}
                item['sanction_list']['sl_authority'] = "Japan Public Security Intelligence Agency Terrorists Ogranisations, Japan"
                item['sanction_list']['sl_url'] = "https://www.moj.go.jp/"
                item['sanction_list']['sl_host_country'] = "Japan"
                item['sanction_list']['sl_type'] = "Sanctions"
                item['sanction_list']['sl_source'] = "Japan Public Security Intelligence Agency Terrorists Ogranisations, Japan"
                item['sanction_list']['sl_description'] = "Japan Public Security Intelligence Agency Terrorists Ogranisations, Japan"
                item['sanction_list']['watch_list'] = "APAC Watchlists"
                item['sanction_list']['list_id'] = "JPN_T30104"
                self.final_list.append(item)
        except Exception as e:
            print(e)
        try:
            with open(f'{self.outputpath}/{self.date}_output_sng-771.ch.json', "w") as outfile:
                json.dump(self.final_list, outfile, indent=4)

        except Exception as e:
            print(e)
        except Exception as e:
            print(e)


temp = Data()
temp.parsing()
# temp.compare()
# temp.uploadToS3()
