import requests
from scrapy.http import HtmlResponse
import datetime
import json
import os
import boto3
import copy
from os.path import exists
import hashlib

class fcraonline():
    try:
        final_list = []
        date = datetime.date.today()
        yesterday = date - datetime.timedelta(days=1)
        root = "/home/ubuntu/sanctions-scripts/FCRA-ONLINE/"
        inputpath = f'{root}InputFiles'
        outputpath = f'{root}OutputFiles'
        Differencepath = f'{root}DifferenceFiles'
        last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        rmpath = f'{root}RemovedFiles'
        lp_path = f"{root}FCRA_online_log.csv"
        lp_name = "FCRA_online_log.csv"
        os.mkdir(rmpath)
        os.mkdir(inputpath)
        os.mkdir(outputpath)
        os.mkdir(Differencepath)
    except:
        pass

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

    def uploadToS3(self):
        try:
            print("uploading files to s3")
            s3 = boto3.client('s3')
            # s3.upload_file(f'{self.inputpath}/{self.date}_input_eu.xml', "sams-scrapping-data",
            #                f"EU/Original/{self.date}_input_eu.xml")
            s3.upload_file(f'{self.outputpath}/{self.date}_output_fcraonline.json', "sams-scrapping-data",
                           f"fcra/Parced/{self.date}_output_fcraonline.json")
            s3.upload_file(f'{self.Differencepath}/{self.date}_Difference_Output_fcraonline.json', "sams-scrapping-data",
                           f"fcra/Diffrance/{self.date}_Difference_Output_fcraonline.json")
            s3.upload_file(f'{self.rmpath}/{self.date}_RemovedData_File_fcraonline.json', "sams-scrapping-data",
                           f"fcra/Removed/{self.rmpath}/{self.date}_RemovedData_File_fcraonline.json")
            s3.upload_file(f'{self.lp_path}', "sams-scrapping-data",
                           f"fcra/{self.lp_name}")
            print("uploaded files to s3")
        except Exception as e:
            print(e)

    def compare(self):
        try:
            with open(f'{self.outputpath}/{self.yesterday}_output_fcraonline.json', 'rb') as f:
                data = f.read()
        except:
            # print("---------------------Alert--------------------------")
            print(f"There is not any old file for date: {self.yesterday.ctime()}")
            # print("----------------------------------------------------")
            data = "No DATA"
        old_list= []
        if data != "No DATA":
            old_list = json.loads(data)

        new_list = copy.deepcopy(self.final_list)
        new_profiles = []
        removed_profiles = []
        updated_profiles = []

        old_dict = {}
        for val in old_list:
            old_dict[val["uid"]] = val

        with open(f'{self.outputpath}/{self.date}_output_fcraonline.json', 'rb') as f:
            data = f.read()
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
        #NOTE: ---------- META LOg FIlE --------------------------
        try:
            with open('/home/ubuntu/meta-scrap-log.json', 'rb') as f:
                logd = f.read()
                pred = json.loads(logd)
                pred.append({
                    "dag" : "FCRA-ONLINE",
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


        with open(f'{self.Differencepath}/{self.date}_Difference_Output_fcraonline.json', "w") as outfile:
            json.dump(new_profiles + updated_profiles, outfile)

        with open(f'{self.rmpath}/{self.date}_RemovedData_File_fcraonline.json', "w") as outfile:
            json.dump(removed_profiles, outfile)

        if exists(self.lp_path):
            with open(f'{self.lp_path}',"a") as outfile:
                passing = f"{self.last_updated_string},{self.date}_input_fcraonline.json,{self.date}_output_fcraonline.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_Difference_Output_fcraonline.json',{self.date}_RemovedData_File_fcraonline.json'\n"
                outfile.write(passing)
        else:
            with open(f'{self.lp_path}',"a") as outfile:
                pass_first = "date,inputfile,outputfile,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
                passing = f"{self.last_updated_string},{self.date}_input_fcraonline.json,{self.date}_output_fcraonline.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_Difference_Output_fcraonline.json',{self.date}_RemovedData_File_fcraonline.json'\n"
                outfile.write(pass_first)
                outfile.write(passing)


    def parseing(self):
        try:
            url = "https://fcraonline.nic.in/fc_deemed_asso_list.aspx"

            payload = {}
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
                'Cookie': 'ASP.NET_SessionId=ab46ab34-52dd-4f10-b980-a22aac987265'
            }

            res = requests.get(url, headers=headers, data=payload)
            response = HtmlResponse(url="example.com",body=res.content)
            try:
                state_value = response.xpath('//*[@id="up_state"]//option//@value').extract()
                state_value.pop(0)
                for i in state_value:

                    try:
                        url = "https://fcraonline.nic.in/fc_deemed_asso_list.aspx"

                        payload = f"__LASTFOCUS=&__EVENTTARGET=&__EVENTARGUMENT=&__VIEWSTATE=6HpqO6XWzmT3tkh9W6s0jlM0b4auQHLlGkuuM1UQ9GLS9xWYkj01r1lTnI6IwxKtVWI5ivpFyol4REYD%2BRSz2fcbPWOP19RAOXSstjk6uVbgvKXkKK1YDLNSVrNzA9jZMEPw5BFPVF6Oo%2BFGsuxRBxSq9iLvtkrU8FswhJpLNBj9b3lWHmFplRLVuc6VE%2FTrNreq2U4VQ4e7s4j%2BYqpSrQ8LbI88tcDMsEVq3kMVvH0VkPPAGe7c02fEBxcVY8SKAPZqj3nPh9%2F3Ki5tyv91QoW3X4eC8o1r87%2Bp5zTNT7hY%2FowKPNpVcuzx50U4%2F5mcpVkTRQ%2Bf%2B9EN8dvh3INPK1U04q2YIMCnUAUx8tCHcFJDTvkIXfleN2O%2BZr7UbWDyfAfgvabnnn60AjXLBLiMQ7Sv%2FqOKNNrtuThsiVX29JkkPLX5WBGl8zPqd2HrgMTRKY0FdHwtyiyQuIRKqh%2Fy3Qoohg%2B78bsM1tcf%2BnGoH05k83RMzhbRJzKUA%2FCTSVcX9H35ElnCzP4AuDLITZIv50M7Cdh2yHl%2BJ9acL15ncYZ3cb2vtiVgUuDPD056JHA7k2L30%2Fnjs2P7nhi3Sl7gzlxcE9%2FWiDSmdvMVxrpKC2Ld91vkW2y5Q%2FV9UuFCx4yqqKNZxqcPXHKr085cDg%2BGKpiOHBLeV3FkolyCN9usWoghjs86NjMK0RJvAVlbXLePxY%2Bz13mcmSXsQW%2F%2BG8QpxDLk5qGNEhKu%2FxAYeO0dbcWa3B9Kh%2BzBIqDp7coR7xsNpbjyFecvAgnkL8LelfLy54fydJkIYhZwtc30ssFe21Mlx0hOhJygf9oU0g8DBDYyJkScGBEj%2BjK%2F1d1NJEFg2GZvfUSFS%2FyGqWHvWOcfnPDviKodZWIcCUmxEWDY3V%2FBS6NB6m5z%2BtiNWt5WFN7IE9OGnTUAA6vzlqV81Vzk94domEPYAhPTQNHwMecI6OIwSELcbSi%2BtYur74yVBegUVkXyw4vfRWXo3psfE8GMrPYRubHg%2BHiKTHBwdWZZVs2jRCa0bvfESuFTAZainP3IQFk9dBV29CTS5CB3hwV6AKECZT5ZI4HmpE3qdXoUM6aNBSkMiFdqgFyTyIRIkSj8xURkr9fHLIC8W7eADBQ6%2BjBrBuYRJDis6tK8xKoE9ksWSrEO3iMla%2B0zxeY1%2FfwM0hz4QNfubx2KoJUS1ToQh7MUrO7UgUVqedkNk0B7xo3giwnUAHnrbXEvVoZhE7gZEjl6dkQ7yLPRSJ9afO2ZDR0a7DmZnh1T6K6fieRxl4CZpeGWhELIXdI5W0x04k0jBu%2FHG1rqtgGfDRR1Eak9f3k2l5qsDCgmL%2B57uKxtwQXZ3X9FeEgvORYtXrKFqXE6V8DF8ZDiE3KCN7Zpna0EPLZv50Wy1CoiRxTmLxfKWXqyekJLQUlF0lwts0WVvqoSQia80i0oCgWfpVVWbtaM9dn0fDxuK2JvbvDlLudfOH9dYcSvtxj8KVsQNfkE37OKKiYyU09tMypNH8Pzrw5uOcc9xzJQvXIqQ1qeWhuY9jRUcdDbXBYDcjr3qFgWBvsGr2OZxViph6mzwk6boIKUnAmNBplQWWAijeGEB3MP%2BOSBhHie87HNhTieECxcepWw0KNTDpvQF3zNNN6ix%2FxRHP66e6FZQooW%2BRBJI%2FxA8AfbBGBQqOs4W6GT3AQYenRkQ4sv57oMiLkK0KYqHbO6GqvaC3hgiWarBgGfmHrtXxdZ6qpX9JQFCn16Aw9XlcSXmNNVNiVY6sRJ7QDRtq%2Bjb%2F8cem1gLmq5BnGjJXEue5Mxo7%2Bwd0b4hewJ3m8ATG9pCkfG6L0%3D&__VIEWSTATEGENERATOR=6E8CE4FE&__VIEWSTATEENCRYPTED=&h_hash=&h_hash_retype=&ddlstate={i}&ddldist=0&txtsearch=&rdlist=0&btsubmitt=Submit"
                        headers = {
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
                            'Cookie': 'ASP.NET_SessionId=ab46ab34-52dd-4f10-b980-a22aac987265'
                        }

                        ress = requests.post(url, headers=headers, data=payload)
                        responsee = HtmlResponse(url="example.com",body=ress.content)
                        last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                        try:
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
                                    address = ii.xpath('.//td[4]//text()').extract_first(default="").strip()
                                except:
                                    address = ""
                                try:
                                    nature = ii.xpath('.//td[5]//text()').extract_first(default="").strip()
                                except:
                                    nature = ""
                                try:
                                    state = responsee.xpath('//*[contains(text(),"State:")]/..//font//text()').extract_first(default="").strip()
                                except:
                                    state = ""
                                item = {}
                                item["uid"] = hashlib.sha256(((name + address + state + "FCRA_ONLINE").lower()).encode()).hexdigest()
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
                                add['complete_address'] = address + state + "India"
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
                                item['sanction_list']['sl_source'] = "The Foreign Contribution (Regulation) Act, List of Registered Associations deemed to have ceased - India"
                                item['sanction_list']['sl_description'] = "List of Registered Associations deemed to have ceased BY The Foreign Contribution (Regulation) Act of Ministry of Home Affairs, India."
                                item['sanction_list']['watch_list'] = "India Watchlists"
                                item['list_id'] = "IND_E20004"
                                self.final_list.append(item)
                        except Exception as e:
                            print(e)
                    except Exception as e:
                        print(e)
                try:
                    with open(f'{self.outputpath}/{self.date}_output_fcraonline.json', "w") as outfile:
                        json.dump(self.final_list, outfile, indent=2)
                except Exception as e:
                    print(e)
            except Exception as e:
                print(e)
            # print(response.text)
        except Exception as e:
            print(e)

temp = fcraonline()
temp.parseing()
temp.compare()
temp.uploadToS3()