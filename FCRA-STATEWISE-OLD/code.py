import requests
from scrapy.http import HtmlResponse
import datetime
import os
import json
import boto3
import copy
import hashlib
from os.path import exists

class fcraonline():
    try:
        final_list = []
        date = datetime.date.today()
        yesterday = date - datetime.timedelta(days=1)
        root = "/home/ubuntu/sanctions-scripts/FCRA-STATEWISE-OLD/"
        inputpath = f'{root}InputFiles'
        outputpath = f'{root}OutputFiles'
        Differencepath = f'{root}DifferenceFiles'
        rmpath = f'{root}RemovedFiles'
        last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        lp_path = f"{root}FCRA_statewise_old_log.csv"
        lp_name = "FCRA_statewise_old_log.csv"
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
            # s3.upload_file(f'{self.inputpath}/{self.date}_input_eu.xml', "sams-scrapping-data",
            #                f"EU/Original/{self.date}_input_eu.xml")
            s3.upload_file(f'{self.outputpath}/{self.date}_output_fcraonline_fc1a_Satewise_old.json', "sams-scrapping-data",
                           f"FCRA-STATEWISE-OLD/Parced/{self.date}_output_fcraonline_fc1a_Satewise_old.json")
            s3.upload_file(f'{self.Differencepath}/{self.date}_Difference_output_fcraonline_fc1a_Satewise_old.json', "sams-scrapping-data",
                           f"FCRA-STATEWISE-OLD/Diffrance/{self.date}_Difference_output_fcraonline_fc1a_Satewise_old.json")
            s3.upload_file(f'{self.rmpath}/{self.date}_RemovedData_File_fcraonline_fc1a_Satewise_old.json', "sams-scrapping-data",
                           f"FCRA-STATEWISE-OLD/Removed/{self.date}_RemovedData_File_fcraonline_fc1a_Satewise_old.json")
            s3.upload_file(f'{self.lp_path}', "sams-scrapping-data",
                        f"FCRA-STATEWISE-OLD/{self.lp_name}")
            print("uploaded files to s3")
        except Exception as e:
            print(e)

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

    def compare(self):
        try:
            with open(f'{self.outputpath}/{self.yesterday}_output_fcraonline_fc1a_Satewise_old.json', 'rb') as f:
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
        #NOTE: ---------- META LOg FIlE --------------------------
        try:
            with open('/home/ubuntu/meta-scrap-log.json', 'rb') as f:
                logd = f.read()
                pred = json.loads(logd)
                pred.append({
                    "dag" : "FCRA-STATEWISE-OLD",
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

        with open(f'{self.Differencepath}/{self.date}_Difference_output_fcraonline_fc1a_Satewise_old.json', "w") as outfile:
            json.dump(new_profiles + updated_profiles, outfile)

        with open(f'{self.rmpath}/{self.date}_RemovedData_File_fcraonline_fc1a_Satewise_old.json', "w") as outfile:
            json.dump(removed_profiles, outfile)

        if exists(self.lp_path):
            with open(f'{self.lp_path}',"a") as outfile:
                passing = f"{self.last_updated_string},{self.date}_output_fcraonline_fc1a_Satewise_old.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_Difference_output_fcraonline_fc1a_Satewise_old.json',{self.date}_RemovedData_File_fcraonline_fc1a_Satewise_old.json'\n"
                outfile.write(passing)
        else:
            with open(f'{self.lp_path}',"a") as outfile:
                pass_first = "date,outputfile,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
                passing = f"{self.last_updated_string},{self.date}_output_fcraonline_fc1a_Satewise_old.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_Difference_output_fcraonline_fc1a_Satewise_old.json',{self.date}_RemovedData_File_fcraonline_fc1a_Satewise_old.json'\n"
                outfile.write(pass_first)
                outfile.write(passing)

    def parsing(self):
        try:
            url = "https://fcraonline.nic.in/fc1a_Satewise_old.aspx"

            payload = {}
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
                'Cookie': 'ASP.NET_SessionId=5374f19a-ceb4-4596-92d8-4dbe59b4e301'
            }

            res = requests.get(url, headers=headers, data=payload)
            response = HtmlResponse(url="example.com", body=res.content)
            try:
                year = response.xpath('//*[@name="ddlyear"]//option//@value').extract().pop(0)
                for i in year:
                    try:
                        url = "https://fcraonline.nic.in/fc1a_Satewise_old.aspx"

                        payload = f"__LASTFOCUS=&__EVENTTARGET=&__EVENTARGUMENT=&__VIEWSTATE=6pTwkotT4LbdynPMAoVcSeBBKCB6X5Cn96npxS6%2B5quX8Mh2oTAKGtBQdOPCIxuaHVNdl9arOQar9occv74b%2FjCEx8jMQ6shC%2BWVGGaGd4mzbGMXhaIQjMfJ6DKsax4lbj%2FJJ88C1iyzUXl8xfeoCZnJNMEAXlAcvgwtYI2k0B961sB4SEDRoEdqMrym0cj4muLj20Ijnf87JBy3gaGOFPX9YvKovOTnB%2BPFmJMpE3NjE47I4zOBq6fhnUpoK4NnPoO%2B9bwoINjx0KTyPTqRF58SY%2FTtNxD2CDVd6MGt4k2IHBztPrjkBI6eraXYN14xq88vY8mVhgrCgoumuEpqPfxF5IPs34zfXoLy4s%2FNl5GSPQzi0oJPiSVA1J2OS8AN1aVDkqEv2T3S2Q74vyDk6B5%2FfAN6B35%2FIm3pgAcdPklUzjk0ru9Wt%2BzuMvQ9GZK54TgqdBcypr%2BxI1JWPMEu7XBKjWRUHdnUQya28yIDfZd185K6v3AACqt6Iy%2FBtOSzXOtQpRhncADLB6gw9UfDJyKoOsnx5bEqj%2Fclp0UXsCEyfa9I0ZUaYS%2B2URIu5be9oLnn8q%2BBBOIotJTPONFed4mRSv3rmuC08qZVV%2Fht31TLjAjMqTqA62hETjA3V%2Fo3Gi%2B1sF01PJgY0b2ZfHqTqnNCl3G08Fparfi8kgXaIPto%2B%2BfKsTkC7TGh3Yx7dv%2BulzJw9D8KLgLuQaFGyPmv8YoshneRrLSix8II7XCg1YioK9GFc40oHS4Np8JOYMr06F6J4efWJfdVzjEpm866SeQ%2FbhELm7l4AePWP2vqreHwhmWzMswBzCXLWgl80AaIfKEfuNQSAtQZFyTjAGX1KSQUHyQuXbtbzc7wdassTcpsrg6TaI%2B7OzEBtFxLpKzu7H17faNvuIKFOVCQL9T7kmfHaMjDuYVAR6fhZnNE%2BupyeQu%2Bv0W%2BvX9xuBVpqvIVsrDPx0b0blh3qvtGV8VdcXOEsFhQYTnGV0gKzw0HNqt0wWX5ook07xEZ1wYYl8X5gw7gXWr%2F%2FwOCJwNOPlvlp%2FP%2FLUYMRYrgSqsHC3cJgZOLIQfbtB68XVzAvc8EDMRk5QjWnjXX0TMnkFnfEQvMUVNyXzp%2F%2B0a8pZHrf7bWx8Lcun1Xnew8RZwdvhb%2BMXj2es4HnOtBZRHZwBuYkzascPQrN%2FlVgNTU3XAlqZQKQPch%2BfXXfGCbZcFJ5Pp6P7UpgR6y6716INa9sYX1bKyALgySUcvCx2BEo9%2BKb3%2FWehQSs9TcAI3upudVkwoLz1QX%2FRCnHAdMw%2BJkg6B2a2pRM%2Fp7TmZpW0JBVy2U0ewfqRtl5fCfBu4dTC%2Bi9XRhVlzAu%2B%2FLElzByCVHUx6NwdflwhTeSpfUfDe71z1rzZI%2BOLv%2FGDW%2BNNMxi2WoTzPF5qt18NmJBh5OdrC34jHSIYfqkVnZjDe8lVeA0kxlcGdsTF9IP%2BxPqvcnRrMQwxipTeLCsA96og%3D%3D&__VIEWSTATEGENERATOR=2A812FEF&__VIEWSTATEENCRYPTED=&h_hash=&h_hash_retype=&ddlyear={i}&ddlstate=0&btsubmitt=Submit"
                        headers = {
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
                            'Cookie': 'ASP.NET_SessionId=5374f19a-ceb4-4596-92d8-4dbe59b4e301'
                        }
                        ress = requests.post(url, headers=headers, data=payload)
                        responsee = HtmlResponse(url="example.com", body=ress.content)
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
                                    address = ii.xpath('.//td[4]//text()').extract_first(default="").strip()
                                except:
                                    address = ""
                                try:
                                    year = ii.xpath('.//td[5]//text()').extract_first(default="").strip()
                                except:
                                    year = ""
                                item = {}
                                item["uid"] = hashlib.sha256(((name  + address + "FCRA_ONLINE_old_statwise").lower()).encode()).hexdigest()
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
                                add['complete_address'] = address + "India"
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
                                item['sanction_list']['sl_source'] = "The Foreign Contribution (Regulation) Act, Prior Permission Sanctioned List, India"
                                item['sanction_list']['sl_description'] = "List of Registered Associations deemed to have ceased BY The Foreign Contribution (Regulation) Act of Ministry of Home Affairs, India."
                                item['sanction_list']['watch_list'] = "India Watchlists"
                                item['list_id'] = "IND_E20004"
                                self.final_list.append(item)
                        except Exception as e:
                            print(e)

                    except Exception as e:
                        print(e)
                try:
                    with open(f'{self.outputpath}/{self.date}_output_fcraonline_fc1a_Satewise_old.json', "w") as outfile:
                        json.dump(self.final_list, outfile,indent=2)
                except Exception as e:
                    print(e)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)

temp = fcraonline()
temp.parsing()
temp.compare()
temp.uploadToS3()