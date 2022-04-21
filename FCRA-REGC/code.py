import requests
import json
import datetime
import os
import boto3
import copy
from scrapy.http import HtmlResponse
import hashlib
from os.path import exists

class Data():
    try:
        final_list = []
        date = datetime.date.today()
        yesterday = date - datetime.timedelta(days=1)
        root = "/home/ubuntu/sanctions-scripts/FCRA-REGC/"
        inputpath = f'{root}InputFiles'
        outputpath = f'{root}OutputFiles'
        Differencepath = f'{root}DifferenceFiles'
        rmpath = f'{root}RemovedFiles'
        last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        lp_path = f"{root}FCRA_regc_old_log.csv"
        lp_name = "FCRA_regc_old_log.csv"
        os.mkdir(inputpath)
        os.mkdir(outputpath)
        os.mkdir(Differencepath)
        os.mkdir(rmpath)

    except:
        pass
    def uploadToS3(self):
        try:
            print("uploading files to s3")
            s3 = boto3.client('s3')
            # s3.upload_file(f'{self.inputpath}/{self.date}_input_eu.xml', "sams-scrapping-data",
            #                f"EU/Original/{self.date}_input_eu.xml")
            s3.upload_file(f'{self.outputpath}/{self.date}_output_fcraonline_fc8_cancel_query.json', "sams-scrapping-data",
                           f"FCRA-REG-CANCEL/Parced/{self.date}_output_fcraonline_fc8_cancel_query.json")
            s3.upload_file(f'{self.Differencepath}/{self.date}_Difference_Output_fcraonline_fc8_cancel_query.json', "sams-scrapping-data",
                           f"FCRA-REG-CANCEL/Diffrance/{self.date}_Difference_Output_fcraonline_fc8_cancel_query.json")
            s3.upload_file(f'{self.rmpath}/{self.date}_RemovedData_File_fcraonline_fc8_cancel_query.json', "sams-scrapping-data",
                           f"FCRA-REG-CANCEL/Removed/{self.date}_RemovedData_File_fcraonline_fc8_cancel_query.json")
            s3.upload_file(f'{self.lp_path}', "sams-scrapping-data",
                           f"FCRA-REG-CANCEL/{self.lp_name}")
            print("uploaded files to s3")
        except Exception as e:
            print(e)

    def compare(self):
        try:
            with open(f'{self.outputpath}/{self.yesterday}_output_fcraonline_fc8_cancel_query.json', 'rb') as f:
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
            raise ValueError("Error : Data Parsing Error.... fix it quick ⚒️")

        with open(f'{self.Differencepath}/{self.date}_Difference_Output_fcraonline_fc8_cancel_query.json', "w") as outfile:
            json.dump(new_profiles + updated_profiles, outfile)

        with open(f'{self.rmpath}/{self.date}_RemovedData_File_fcraonline_fc8_cancel_query.json', "w") as outfile:
            json.dump(removed_profiles, outfile)

        if exists(self.lp_path):
            with open(f'{self.lp_path}',"a") as outfile:
                passing = f"{self.last_updated_string},{self.date}_output_fcraonline_fc8_cancel_query.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_Difference_Output_fcraonline_fc8_cancel_query.json',{self.date}_RemovedData_File_fcraonline_fc8_cancel_query.json'\n"
                outfile.write(passing)
        else:
            with open(f'{self.lp_path}',"a") as outfile:
                pass_first = "date,outputfile,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
                passing = f"{self.last_updated_string},{self.date}_output_fcraonline_fc8_cancel_query.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_Difference_Output_fcraonline_fc8_cancel_query.json',{self.date}_RemovedData_File_fcraonline_fc8_cancel_query.json'\n"
                outfile.write(pass_first)
                outfile.write(passing)

    def alin_gen(self,nmstri):
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

    def parsing(self):
        try:
            url = "https://fcraonline.nic.in/fc8_cancel_query.aspx"

            payload = {}
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
                'Cookie': 'ASP.NET_SessionId=5374f19a-ceb4-4596-92d8-4dbe59b4e301'
            }

            res = requests.get(url, headers=headers, data=payload)
            response = HtmlResponse(url="example.com",body=res.content)
            try:
                state_no = response.xpath('//*[@name="ddn_state"]//option//@value').extract().pop(0)
                for i in state_no:
                    try:
                        url = "https://fcraonline.nic.in/fc8_cancel_query.aspx"

                        payload = f"__VIEWSTATE=y%2BJZV4RQ%2BXd3ROVaTCeVWANGIoOlXWYu1FzjDHlbm8Uj%2B7x4If9XcPvczEPLibdkYujBAFDon8qw05H0SfPZ0xLldZaeUWNPJX%2BXjXHoSEwe4eZfwd%2BU94eUZL6UA%2BPjaz2r1tv%2BFcjpwFw66CLP9HTs3bfBk2ke4xQ92p%2Bp1Z%2FqRVmU4Fn%2FRwNeSpRVdxeHqVM5MazH7IdRV4sawH%2BsIxdGdkc0yv2hXDnm4cTzFFtFObK4AxBAk4hnzig9pYU%2BQmv6wfoUqDT6ObmY8nl04LY8%2BkkaVrHJOPlZHvpIOjol%2FCDLzOUHtml9dBy9MA9S2uEE8PjQF3IQ26ngbrv9b6d6zdhIxq%2FcRbpGRp4PAeTPBKwAaMcn51JXOt4n6g6Lu1c15%2BoGNT%2BDeim3xOqOhkYs6KOIZ%2B85iJHhUogWw%2FdQDfdBFIg3KQP0XQZOfBLM%2Baly0T871Gcih6VCpaglLeGqZkujaPjaIGKoOlgz4BEf7th0jCpHfTC42vhZaZl7kAFHaAgGIk1bEhJjPBZqFxwxirBM8ZMGrVSsdqxEtyD%2BHkX2PL4IeAcyoUw8pTrZfmzQHrJu00BwxAHW3oq%2Fe2s0Rowjd1qGJ%2BYFxTuTYQbiLj95BB9EnHGMzPM%2F3vx5XnRMkUMIFUKRirZtpRCNEU5g4TpE56lS%2Br9Czlz5oVnpPRT%2BBUM1hf4kHG4mLMt%2B2lVkcb2FXKQ1Z8GbTSMjDpbVi5qmKQ4q0bdjK8eXCSP9%2Bb%2BNo2qGKRkGc7aWh%2F4GMMKzkc%2Fuji5D2oiFFlScfYrclgqqGP%2BV%2BZuAL1ycUWPJAodCu4VZHanON0CoB%2BvDxKC88zEIXIqqGgTVfXuWHq5nvvDkv9bW1y61WUTZspqp6FF0xHhI86UOmWG5R8z5rr2wpFSgnSCgikEh8KUREgItJkVtj4wAYkqBceMecsBhw%2Fc2xHXqpcuN2W94im0lnn5DEE9Z7ktE6rjF8QFgobKLFWLfL2P6SRmKLUqCMKVLyeyYBhT9qekxBt%2FfgpeFZgkeoaBJ4DXcPybcP9SGF3fgelI%2F9D3rj7n%2BmKuWiHZIJbHPNCb7ap%2BBspNYEU8dfrnY8tfKPpWlqDJgPtADrSIXMKvBK5apr5rnmQ6X%2ByBQCY7SsQ14lTKOziVBaRPesZYdQ3BG%2BXSYtVgYLfahksn6fKQDG7Jd%2Bm4E8BqHsfUPhWJA&__VIEWSTATEGENERATOR=1E08B2B2&__VIEWSTATEENCRYPTED=&h_hash=&h_hash_retype=&TxtRand=&ddn_state={i}&search=Search"
                        headers = {
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'Sec-Fetch-Mode': 'navigate',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
                            'Cookie': 'ASP.NET_SessionId=5374f19a-ceb4-4596-92d8-4dbe59b4e301'
                        }

                        ress = requests.post(url, headers=headers, data=payload)
                        responsee = HtmlResponse(url="example.com",body=ress.content)
                        try:
                            last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                            block = responsee.xpath('//*[@class="container fixed-height"]//tr')
                            block.pop(0)
                            for ii in block:
                                try:
                                    name = ii.xpath('./td[3]//text()').extract_first(default="").strip()
                                except:
                                    name = ""
                                try:
                                    reg_no = ii.xpath('.//td[2]//text()').extract_first(default="").strip()
                                except:
                                    reg_no = ""
                                try:
                                    year = ii.xpath('.//td[5]//text()').extract_first(default="").strip()
                                except:
                                    year = ""
                                try:
                                    state = responsee.xpath('//*[contains(text(),"State:")]/..//font//text()').extract_first(default="").strip()
                                except:
                                    state = ""

                                item = {}
                                item["uid"] = hashlib.sha256(((name + state + "FCRA_ONLINE_old_statwise").lower()).encode()).hexdigest()
                                item['name'] = name
                                item['alias_name']= self.alin_gen(name)
                                print(name)
                                item['country'] = []
                                item['country'].append("India")
                                item['list_type'] = "Entity"
                                item['last_updated'] = last_updated_string
                                item['entity_details'] = {}
                                item['nns_status'] = False
                                item['address'] = []
                                add = {}
                                add['complete_address'] =  state + "India"
                                add['state'] = state
                                add['country'] = "India"
                                item['address'].append(add)
                                item['documents'] = {}
                                item['documents']['Registration'] = reg_no
                                item['comment'] = ""
                                item['sanction_list'] = {}
                                item['sanction_list']['sl_authority'] = "Ministry of Home Affairs, India"
                                item['sanction_list']['sl_url'] = "https://fcraonline.nic.in/fc_deemed_asso_list.aspx"
                                item['sanction_list']['sl_host_country'] = "India"
                                item['sanction_list']['sl_type'] = "Sanctions"
                                item['sanction_list']['sl_source'] = "The Foreign Contribution (Regulation) Act, Registration Cancelled List, India"
                                item['sanction_list']['sl_description'] = "List of Registered Associations deemed to have ceased BY The Foreign Contribution (Regulation) Act of Ministry of Home Affairs, India."
                                item['sanction_list']['watch_list'] = "India Watchlists"
                                item['list_id'] = "IND_E20004"
                                self.final_list.append(item)
                        except Exception as e:
                            print(e)
                    except Exception as e:
                        print(e)
                try:
                    with open(f'{self.outputpath}/{self.date}_output_fcraonline_fc8_cancel_query.json', "w") as outfile:
                        json.dump(self.final_list, outfile,indent=2)
                except Exception as e:
                    print(e)
            except Exception as e:
                print(e)
            # print(res.text)
        except Exception as e:
            print(e)

temp = Data()
temp.parsing()
temp.compare()
temp.uploadToS3()