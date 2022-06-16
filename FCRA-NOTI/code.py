import requests
import json
from scrapy.http import HtmlResponse
import datetime
import os
import copy
import boto3
import hashlib
from os.path import exists

class Data():
    try:
        final_list = []
        date = datetime.date.today()
        yesterday = date - datetime.timedelta(days=1)
        root = "/home/ubuntu/sanctions-scripts/FCRA-NOTI/"
        inputpath = f'{root}InputFiles'
        outputpath = f'{root}OutputFiles'
        Differencepath = f'{root}DifferenceFiles'
        rmpath = f'{root}RemovedFiles'
        last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        lp_path = f"{root}fcra_notified_log.csv"
        lp_name = "fcra_notified_log.csv"
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
            s3.upload_file(f'{self.outputpath}/{self.date}_output_fcraonline_fc3_Notfiledwithadd.json', "sams-scrapping-data",
                           f"FCRA-NOTI/Parced/{self.date}_output_fcraonline_fc3_Notfiledwithadd.json")
            s3.upload_file(f'{self.Differencepath}/{self.date}_Difference_output_fcraonline_fc3_Notfiledwithadd.json', "sams-scrapping-data",
                           f"FCRA-NOTI/Diffrance/{self.date}_Difference_output_fcraonline_fc3_Notfiledwithadd.json")
            s3.upload_file(f'{self.rmpath}/{self.date}_RemovedData_File_fcraonline_fc3_Notfiledwithadd.json', "sams-scrapping-data",
                           f"FCRA-NOTI/Removed/{self.date}_RemovedData_File_fcraonline_fc3_Notfiledwithadd.json")
            s3.upload_file(f'{self.lp_path}', "sams-scrapping-data",
                           f"FCRA-NOTI/{self.lp_name}")
            print("uploaded files to s3")
        except Exception as e:
            print(e)

    def compare(self):
        try:
            with open(f'{self.outputpath}/{self.yesterday}_output_fcraonline_fc3_Notfiledwithadd.json', 'rb') as f:
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
            raise ValueError("Error : Data Parsing Error.... fix it quickly ⚒️")


        with open(f'{self.Differencepath}/{self.date}_Difference_output_fcraonline_fc3_Notfiledwithadd.json', "w") as outfile:
            json.dump(new_profiles + updated_profiles, outfile)

        with open(f'{self.rmpath}/{self.date}_RemovedData_File_fcraonline_fc3_Notfiledwithadd.json', "w") as outfile:
            json.dump(removed_profiles, outfile)


        if exists(self.lp_path):
            with open(f'{self.lp_path}',"a") as outfile:
                passing = f"{self.last_updated_string},{self.date}_output_fcraonline_fc3_Notfiledwithadd.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_Difference_output_fcraonline_fc3_Notfiledwithadd.json',{self.date}_RemovedData_File_fcraonline_fc3_Notfiledwithadd.json'\n"
                outfile.write(passing)
        else:
            with open(f'{self.lp_path}',"a") as outfile:
                pass_first = "date,outputfile,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
                passing = f"{self.last_updated_string},{self.date}_output_fcraonline_fc3_Notfiledwithadd.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_Difference_output_fcraonline_fc3_Notfiledwithadd.json',{self.date}_RemovedData_File_fcraonline_fc3_Notfiledwithadd.json'\n"
                outfile.write(pass_first)
                outfile.write(passing)

    def parsing(self):
        url = "https://fcraonline.nic.in/fc3_Notfiledwithadd.aspx"

        payload = {}
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
            'Cookie': 'ASP.NET_SessionId=7b7690d4-05db-496f-85b5-0d4f4ca6a782'
        }

        res = requests.request("GET", url, headers=headers, data=payload)
        response = HtmlResponse(url="example.com",body=res.content)
        html_file = open("fcra_noti.html","w")
        html_file.write(response.body.decode("utf-8"))
        html_file.close()
        # year = response.xpath('//select[@id="ddlyear"]//option/@value').extract()
        # print(year)
        # print(response.xpath("//div['div_main']").getall())

        exit()
        year.pop(0)
        for i in year:
            url = "https://fcraonline.nic.in/fc3_Notfiledwithadd.aspx"

            payload = f"__EVENTTARGET=&__EVENTARGUMENT=&__VIEWSTATE=3DNeLjcyqmAJ8%2BPtnMlj8EeNRvKnpIfi7vCxxd2g%2Bx%2F1VmefOLUbgeGBJunAD4nOQE%2FtpuIY8nWpLkTwQo%2BzbXR2NbXFgaqZo%2BgdLNVLllL3cnEfXsOwby%2FBYOngx5IQlli5gplpf%2FodKtsmJABigLtn1PC18gsF%2FPCa%2Bh20o5BLoS9Ox2hUP%2FhZKcSi1dUHsXGPrCfDIU%2FBzfDDgY1bDEK9HTGgdwPyGcFpLgQHzEF6SICKjk5JngqaF7iVc0RwZqzKxT1w5RLTl0Z93YfQWRcyt9C3DsOvI%2FS0cxwyunJ5sWUiCotuRN9t3s72Tby9jQ06RPoBDM0XcXWaos1v63%2FJGp7798troM6NdY0QkGFe8KlrBcpf0BVTo1TX%2BKYN6nUFAd%2BUJGgsYEfXz5SK%2B1i0CT24NyDqSysvWcR2VVfJdMs6QWST3MmrCt3ffzmFs7r3n49A5aua5IrNGWZxkKs58lpt8FmOwZFyMNxF7iMajokwOUgrgq8qY9XhYyXhV6uQ%2FQ114GXIjegNFTp%2BguQr3gM435XRN2ilpHBIqWt%2FlmMb49N%2BHROgo7KK6NvxdNRymjQkRqnzeL8qb7yhzcY%2BSdxszYbTKJcJW4t%2BMlYjSCR6SwzGYm%2FNITd2y%2BvTwvK%2BfszIU%2F%2Fnqzwbvs47RO57SyyplSOSqVK0JCSMUveCei%2B7CgOmGAe%2Ba5fGTGV6PclQnnQQ0pNsvnO2M83EBuIlmYI1zYgOThn%2BsFQAtIr3n0xYqIChopd38gxnp1jOJQnu8wroq8EWcxDChemQ%2BrbnJ9kNNVJT0SPoQSxibEvchCkdi5T4Fx7LZh7o8exjiGsQsWRUpfY9AuYCOLzM4%2F057IpY9IrJywaNVez90M34eD4JFF54eAS6CTI%2B2cDaDni%2FkGXgie4GgtifsSAsxZ52Kp%2F4wREvmkIpqsa59skSogwXA07XodTvb6z3SeZbzjznncrxvzbHFRpcJwq07k6YlTbuBOpAa9jNhWhBK1k%2F84YtZGmZ6Z67Cx5kHuE0QgXqsA%3D%3D&__VIEWSTATEGENERATOR=9E08422C&__VIEWSTATEENCRYPTED=&h_hash=&h_hash_retype=&ddlblk={i}&ddlState=0&BtnSubmit=Submit"
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
                'Cookie': 'ASP.NET_SessionId=5374f19a-ceb4-4596-92d8-4dbe59b4e301'
            }

            ress = requests.request("POST", url, headers=headers, data=payload)
            responsee = HtmlResponse(url="example.com",body=ress.content)
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
                item = {}
                item["uid"] = hashlib.sha256(((name + address + "FCRA notified").lower()).encode()).hexdigest()
                item['name'] = name
                print(name)
                item["alias_name"] = []
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

        with open(f'{self.outputpath}/{self.date}_output_fcraonline_fc3_Notfiledwithadd.json', "w") as outfile:
            json.dump(self.final_list, outfile,indent=2)

temp = Data()
temp.parsing()
temp.compare()
temp.uploadToS3()