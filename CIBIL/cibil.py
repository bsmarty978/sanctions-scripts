import requests
import json
from scrapy.http import HtmlResponse
import re
import os
import datetime
class Data():

    try:
        counter = 0
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
    def parsing(self):
        try:
            url = "https://suit.cibil.com/suitFiledAccountSearchAction"

            payload = {}
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'kitchify-access-token': 'FB1A9173BAA3D85C5731EBA52454F',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
                'Cookie': 'NSC_WJQ_tvju.djcjm.dpn_100.205=ffffffff096c2ed945525d5f4f58455e445a4a4216cb; __cf_bm=dISsRI09OgXWq79XqTbqBuvAl1Y3EhRa6alSkkgV63M-1653253090-0-AWC/kLyTj8Rdm0s+OG904UXIOVmN8hmYndkCtrFqFCqhs3DYJE3wi2HKZ+emCpkvTWIiL4sb5iabh/oGj9UN1oo='
            }

            res = requests.get(url, headers=headers, data=payload)
            response = HtmlResponse(url="example.com",body=res.content)
            try:
                block = response.xpath('//*[@name="croreAccount"]//option//@value').extract()
                block.pop(0)
                block.pop(0)
                for i in block:
                    try:
                        date = response.xpath('//*[@name="quarterIdCrore"]//option//@value').extract()
                        date_text = response.xpath('//*[@name="quarterIdCrore"]//option//text()').extract()
                        date.pop(0)
                        date_text.pop(0)
                        for d,dt in zip(date,date_text):
                            try:
                                url = "https://suit.cibil.com/loadSuitFiledDataSearchAction"

                                payload = f"quarterIdSummary=0&quarterIdGrantors=0&croreAccount={i}&quarterIdCrore={d}&lakhAccount=0&quarterIdLakh=0&quarterDateStr={dt}&fileType={i}&searchMode={i}"
                                headers = {
                                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                    'accept-encoding': 'gzip, deflate, br',
                                    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                                    'content-type': 'application/x-www-form-urlencoded',
                                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
                                    'Cookie': 'JSESSIONID=C7DFB0D698EFC6C77D4D4D345EFB897F.jvmsuit; NSC_WJQ_tvju.djcjm.dpn_100.205=ffffffff096c2ed945525d5f4f58455e445a4a4216cb; __cf_bm=flIpbyrAtq3qRoVOA_XmaYTYq.frXf_VHZLnzVK3e.M-1653289782-0-AZ1z3YiuymVcI5kjEkZfcKrR+nTWdZtX0jBMh89W79LvYMKXTUWWuGtgg00HT88S9fJvr3wlRd3J2cenz4LWjeI='
                                }

                                ress = requests.request("POST",url, headers=headers, data=payload)
                                responsee = HtmlResponse(url="exmaple.com",body=ress.content)
                                try:
                                    json_data = re.findall("var json = '(.*?)}]}';",responsee.text)[0] + "}]}"
                                    data = json.loads(json_data)
                                    try:
                                        body = data['rows']
                                        for ii in body:
                                            try:
                                                bankid = ii['branchBean']['bankBean']['bankId']
                                            except:
                                                bankid = ""
                                            try:
                                                url = "https://suit.cibil.com/getSuitFiledStateSummaryAction"

                                                payload = f"fileType=2&state=&suitSearchBean.quarterBean.quarterId=85&suitSearchBean.bankBean.bankId={bankid}&summaryState=1&suitSearchBean.summaryType=1&bank="
                                                headers = {
                                                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                                    'accept-encoding': 'gzip, deflate, br',
                                                    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                                                    'content-type': 'application/x-www-form-urlencoded',
                                                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
                                                    'Cookie': 'JSESSIONID=C7DFB0D698EFC6C77D4D4D345EFB897F.jvmsuit; NSC_WJQ_tvju.djcjm.dpn_100.205=ffffffff096c2ed945525d5f4f58455e445a4a4216cb; __cf_bm=flIpbyrAtq3qRoVOA_XmaYTYq.frXf_VHZLnzVK3e.M-1653289782-0-AZ1z3YiuymVcI5kjEkZfcKrR+nTWdZtX0jBMh89W79LvYMKXTUWWuGtgg00HT88S9fJvr3wlRd3J2cenz4LWjeI='
                                                }

                                                resp = requests.request("POST",url, headers=headers, data=payload)
                                                responseee = HtmlResponse(url="example.com",body=resp.content)
                                                try:
                                                    json_dataa = re.findall("var json = '(.*?)}]}';", responseee.text)[0] + "}]}"
                                                    dataa = json.loads(json_dataa)
                                                    try:
                                                        body = dataa['rows']
                                                        for iii in body:
                                                            try:
                                                                stateid = iii['stateBean']['stateId']
                                                            except:
                                                                stateid = ""
                                                            try:
                                                                for page in range(1,4):
                                                                    url = 'https://suit.cibil.com/loadSearchResultPage?fileType=2&suitSearchBeanJson={\"borrowerName\":\"\",\"costAmount\":\"\",\"stateName\":\"\",\"directorName\":\"\",\"branchBean\":null,\"dunsNumber\":\"\",\"city\":\"\",\"bankBean\":{\"bankId\":33,\"bankName\":\"\",\"categoryBean\":{\"categoryId\":0,\"categoryName\":\"\",\"categoryAllotedId\":\"\",\"active\":0,\"enable\":false},\"bankNoRecords\":0,\"bankTotalAmount\":\"\",\"enable\":false,\"active\":0},\"quarterBean\":{\"quarterId\":85,\"quarterDate\":null,\"quarterDateStr\":\"\",\"quarterName\":\"\",\"quarterMonthStr\":\"\",\"quarterYearStr\":\"\",\"isPush\":0},\"stateBean\":{\"stateId\":'+str(stateid)+',\"stateName\":\"\",\"stateNoRecords\":0,\"stateTotalAmount\":\"\",\"category\":\"\",\"enable\":false,\"isActive\":0},\"borrowerAddress\":null,\"borrowerId\":0,\"sort\":0,\"totalRecords\":0,\"totalAmount\":\"\",\"quarterCol\":\"\",\"categoryBean\":null,\"noOFCGs1Cr\":0,\"records1Cr\":0,\"noOFCGs25Lac\":0,\"records25Lac\":0,\"cat\":\"\",\"catGroup\":\"\",\"fromQuarterId\":0,\"toQuarterId\":0,\"partyTypeId\":0,\"quarterId\":0,\"srNo\":\"\",\"userComments\":\"\",\"importDataBean\":null,\"rejected\":0,\"user\":null,\"uploadBatchBean\":null,\"rejectComment\":\"\",\"lastLimit\":0,\"firstLimit\":0,\"reject\":null,\"edit\":null,\"modify\":null,\"editedTotalAmount\":null,\"editedDirectorNames\":null,\"rejectComments\":null,\"updateReject\":\"\",\"userId\":0,\"directorBean\":null,\"isReview\":\"\",\"sortBy\":null,\"sortOrder\":null,\"summaryState\":\"1\",\"summaryType\":\"1\",\"directorId\":0,\"directorSuffix\":\"\",\"dinNumber\":\"\",\"editedDirectorDin\":null,\"dirPan\":\"\",\"editedDirectorPan\":null,\"title\":\"\"}&_search=false&nd=1653255352239&rows=15&page='+str(page)+'&sidx=&sord=asc'

                                                                    payload = {}
                                                                    headers = {
                                                                        'accept': 'application/json, text/javascript, */*; q=0.01',
                                                                        # 'accept-encoding': 'gzip, deflate, br',
                                                                        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                                                                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
                                                                        'x-requested-with': 'XMLHttpRequest',
                                                                        'Cookie': 'JSESSIONID=C7DFB0D698EFC6C77D4D4D345EFB897F.jvmsuit; NSC_WJQ_tvju.djcjm.dpn_100.205=ffffffff096c2ed945525d5f4f58455e445a4a4216cb; __cf_bm=TLyTrrytoQ0oCVpuG8TY2WQipz22KhuRgzeds7BjvXk-1653292214-0-ASmLhgPHsyI2nzwQiaEVDlweshO1MohnSfPdAnEDtGJkCO/fpxhUB9Qu/mbVjs+umft8C4L6H1wQhojfaF7etYw='
                                                                    }

                                                                    respp = requests.request("GET", url,headers=headers,data=payload)
                                                                    loll = json.loads(respp.text)
                                                                    lol = loll['rows']
                                                                    for s in lol:
                                                                        try:
                                                                            name = s['borrowerName']
                                                                        except:
                                                                            name = ""
                                                                        try:
                                                                            address = s['importDataBean']['regaddr']
                                                                        except:
                                                                            address = ""
                                                                        try:
                                                                            Director_name_and_Details = s['directorName']
                                                                        except:
                                                                            Director_name_and_Details = ""
                                                                        try:
                                                                            date = s['quarterBean']['quarterDateStr']
                                                                        except:
                                                                            date = ""
                                                                        try:
                                                                            branch_name = s['branchBean']['branchName']
                                                                        except:
                                                                            branch_name = ""
                                                                        try:
                                                                            bank_name = s['bankBean']['bankName']
                                                                        except:
                                                                            bank_name = ""
                                                                        try:
                                                                            outstanding_amount_in_lacs = s['stateBean']['stateTotalAmount']
                                                                        except:
                                                                            outstanding_amount_in_lacs = ""
                                                                        item = {}
                                                                        item['uid'] = ""
                                                                        item['name'] = name
                                                                        item['alias_name'] = []
                                                                        item['country'] = []
                                                                        item['list_type'] = "Individual"
                                                                        item['last_updated'] = ""
                                                                        item['individual_details'] = {}
                                                                        item['sanction_details'] = {}
                                                                        item['sanction_details']['Date'] = date
                                                                        item['sanction_details']['outstanding_amount_in_lacs'] = outstanding_amount_in_lacs
                                                                        item['sanction_details']['Borrow_Branch_Name'] = branch_name
                                                                        item['sanction_details']['Borrow_Bank_Name'] = bank_name
                                                                        item['family_tree'] = {}
                                                                        item['family_tree']['Associate'] = Director_name_and_Details
                                                                        item['nns_status'] = False
                                                                        item['address'] = []
                                                                        add = {}
                                                                        add['complete_address'] = address
                                                                        add['country'] = "India"
                                                                        item['address'].append(add)
                                                                        item['documents'] = {}
                                                                        item['comment'] = ""
                                                                        item['sanction_list'] = {}
                                                                        item['sanction_list']['sl_authority'] = "Reserve Bank of India, India"
                                                                        item['sanction_list']['sl_url'] = "https://suit.cibil.com/loadSuitFiledDataSearchAction"
                                                                        item['sanction_list']['sl_host_country'] = "India"
                                                                        item['sanction_list']['sl_type'] = "Sanctions"
                                                                        item['sanction_list']['watch_list'] = "India Watchlists"
                                                                        item['sanction_list']['sl_source'] = "Reserve Bank of India Willful Defaulters List, India"
                                                                        item['sanction_list']['sl_description'] = "List of Will full defaulters issued by Reserve Bank of India and sourced from CIBIL Suit Filed Accounts."
                                                                        item['sanction_list']['list_id'] = "IND_E20010"
                                                                        self.final_list.append(item)
                                                            except Exception as e:
                                                                print(e)
                                                    except Exception as e:
                                                        print(e)
                                                except Exception as e:
                                                    print(e)
                                            except Exception as e:
                                                print(e)

                                    except:
                                        body = ""
                                except:
                                    json_data = ""

                            except Exception as e:
                                print(e)
                    except Exception as e:
                        print(e)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)

    def parsing1(self):
        try:
            url = "https://suit.cibil.com/suitFiledAccountSearchAction"

            payload = {}
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
                'Cookie': 'NSC_WJQ_tvju.djcjm.dpn_100.205=ffffffff096c2ed945525d5f4f58455e445a4a4216cb; __cf_bm=dISsRI09OgXWq79XqTbqBuvAl1Y3EhRa6alSkkgV63M-1653253090-0-AWC/kLyTj8Rdm0s+OG904UXIOVmN8hmYndkCtrFqFCqhs3DYJE3wi2HKZ+emCpkvTWIiL4sb5iabh/oGj9UN1oo='
            }

            res = requests.get(url, headers=headers, data=payload)
            response = HtmlResponse(url="example.com",body=res.content)
            try:
                block = response.xpath('//*[@name="lakhAccount"]//option//@value').extract()
                block.pop(0)
                block.pop(0)
                for i in block:
                    try:
                        date = response.xpath('//*[@name="quarterIdCrore"]//option//@value').extract()
                        date_text = response.xpath('//*[@name="quarterIdCrore"]//option//text()').extract()
                        date.pop(0)
                        date_text.pop(0)
                        for d,dt in zip(date,date_text):
                            try:
                                url = "https://suit.cibil.com/loadSuitFiledDataSearchAction"

                                payload = f"quarterIdSummary=0&quarterIdGrantors=0&croreAccount=0&quarterIdCrore=0&lakhAccount={i}&quarterIdLakh={d}&quarterDateStr={dt}&fileType={i}&searchMode={i}"
                                headers = {
                                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                    'accept-encoding': 'gzip, deflate, br',
                                    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                                    'content-type': 'application/x-www-form-urlencoded',
                                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
                                    'Cookie': 'JSESSIONID=C7DFB0D698EFC6C77D4D4D345EFB897F.jvmsuit; NSC_WJQ_tvju.djcjm.dpn_100.205=ffffffff096c2ed945525d5f4f58455e445a4a4216cb; __cf_bm=flIpbyrAtq3qRoVOA_XmaYTYq.frXf_VHZLnzVK3e.M-1653289782-0-AZ1z3YiuymVcI5kjEkZfcKrR+nTWdZtX0jBMh89W79LvYMKXTUWWuGtgg00HT88S9fJvr3wlRd3J2cenz4LWjeI='
                                }

                                ress = requests.request("POST",url, headers=headers, data=payload)
                                responsee = HtmlResponse(url="exmaple.com",body=ress.content)
                                try:
                                    json_data = re.findall("var json = '(.*?)}]}';",responsee.text)[0] + "}]}"
                                    data = json.loads(json_data)
                                    try:
                                        body = data['rows']
                                        for ii in body:
                                            try:
                                                bankid = ii['branchBean']['bankBean']['bankId']
                                            except:
                                                bankid = ""
                                            try:
                                                url = "https://suit.cibil.com/getSuitFiledStateSummaryAction"

                                                payload = f"fileType=2&state=&suitSearchBean.quarterBean.quarterId=85&suitSearchBean.bankBean.bankId={bankid}&summaryState=1&suitSearchBean.summaryType=1&bank="
                                                headers = {
                                                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                                    'accept-encoding': 'gzip, deflate, br',
                                                    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                                                    'content-type': 'application/x-www-form-urlencoded',
                                                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
                                                    'Cookie': 'JSESSIONID=C7DFB0D698EFC6C77D4D4D345EFB897F.jvmsuit; NSC_WJQ_tvju.djcjm.dpn_100.205=ffffffff096c2ed945525d5f4f58455e445a4a4216cb; __cf_bm=flIpbyrAtq3qRoVOA_XmaYTYq.frXf_VHZLnzVK3e.M-1653289782-0-AZ1z3YiuymVcI5kjEkZfcKrR+nTWdZtX0jBMh89W79LvYMKXTUWWuGtgg00HT88S9fJvr3wlRd3J2cenz4LWjeI='
                                                }

                                                resp = requests.request("POST",url, headers=headers, data=payload)
                                                responseee = HtmlResponse(url="example.com",body=resp.content)
                                                try:
                                                    json_dataa = re.findall("var json = '(.*?)}]}';", responseee.text)[0] + "}]}"
                                                    dataa = json.loads(json_dataa)
                                                    try:
                                                        body = dataa['rows']
                                                        for iii in body:
                                                            try:
                                                                stateid = iii['stateBean']['stateId']
                                                            except:
                                                                stateid = ""
                                                            try:
                                                                for page in range(1,4):
                                                                    url = 'https://suit.cibil.com/loadSearchResultPage?fileType=2&suitSearchBeanJson={\"borrowerName\":\"\",\"costAmount\":\"\",\"stateName\":\"\",\"directorName\":\"\",\"branchBean\":null,\"dunsNumber\":\"\",\"city\":\"\",\"bankBean\":{\"bankId\":33,\"bankName\":\"\",\"categoryBean\":{\"categoryId\":0,\"categoryName\":\"\",\"categoryAllotedId\":\"\",\"active\":0,\"enable\":false},\"bankNoRecords\":0,\"bankTotalAmount\":\"\",\"enable\":false,\"active\":0},\"quarterBean\":{\"quarterId\":85,\"quarterDate\":null,\"quarterDateStr\":\"\",\"quarterName\":\"\",\"quarterMonthStr\":\"\",\"quarterYearStr\":\"\",\"isPush\":0},\"stateBean\":{\"stateId\":'+str(stateid)+',\"stateName\":\"\",\"stateNoRecords\":0,\"stateTotalAmount\":\"\",\"category\":\"\",\"enable\":false,\"isActive\":0},\"borrowerAddress\":null,\"borrowerId\":0,\"sort\":0,\"totalRecords\":0,\"totalAmount\":\"\",\"quarterCol\":\"\",\"categoryBean\":null,\"noOFCGs1Cr\":0,\"records1Cr\":0,\"noOFCGs25Lac\":0,\"records25Lac\":0,\"cat\":\"\",\"catGroup\":\"\",\"fromQuarterId\":0,\"toQuarterId\":0,\"partyTypeId\":0,\"quarterId\":0,\"srNo\":\"\",\"userComments\":\"\",\"importDataBean\":null,\"rejected\":0,\"user\":null,\"uploadBatchBean\":null,\"rejectComment\":\"\",\"lastLimit\":0,\"firstLimit\":0,\"reject\":null,\"edit\":null,\"modify\":null,\"editedTotalAmount\":null,\"editedDirectorNames\":null,\"rejectComments\":null,\"updateReject\":\"\",\"userId\":0,\"directorBean\":null,\"isReview\":\"\",\"sortBy\":null,\"sortOrder\":null,\"summaryState\":\"1\",\"summaryType\":\"1\",\"directorId\":0,\"directorSuffix\":\"\",\"dinNumber\":\"\",\"editedDirectorDin\":null,\"dirPan\":\"\",\"editedDirectorPan\":null,\"title\":\"\"}&_search=false&nd=1653255352239&rows=15&page='+str(page)+'&sidx=&sord=asc'

                                                                    payload = {}
                                                                    headers = {
                                                                        'accept': 'application/json, text/javascript, */*; q=0.01',
                                                                        # 'accept-encoding': 'gzip, deflate, br',
                                                                        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                                                                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
                                                                        'x-requested-with': 'XMLHttpRequest',
                                                                        'Cookie': 'JSESSIONID=C7DFB0D698EFC6C77D4D4D345EFB897F.jvmsuit; NSC_WJQ_tvju.djcjm.dpn_100.205=ffffffff096c2ed945525d5f4f58455e445a4a4216cb; __cf_bm=TLyTrrytoQ0oCVpuG8TY2WQipz22KhuRgzeds7BjvXk-1653292214-0-ASmLhgPHsyI2nzwQiaEVDlweshO1MohnSfPdAnEDtGJkCO/fpxhUB9Qu/mbVjs+umft8C4L6H1wQhojfaF7etYw='
                                                                    }

                                                                    respp = requests.request("GET", url,headers=headers,data=payload)
                                                                    loll = json.loads(respp.text)
                                                                    lol = loll['rows']
                                                                    for s in lol:
                                                                        try:
                                                                            name = s['borrowerName']
                                                                        except:
                                                                            name = ""
                                                                        try:
                                                                            address = s['importDataBean']['regaddr']
                                                                        except:
                                                                            address = ""
                                                                        try:
                                                                            Director_name_and_Details = s['directorName']
                                                                        except:
                                                                            Director_name_and_Details = ""
                                                                        try:
                                                                            date = s['quarterBean']['quarterDateStr']
                                                                        except:
                                                                            date = ""
                                                                        try:
                                                                            branch_name = s['branchBean']['branchName']
                                                                        except:
                                                                            branch_name = ""
                                                                        try:
                                                                            bank_name = s['bankBean']['bankName']
                                                                        except:
                                                                            bank_name = ""
                                                                        try:
                                                                            outstanding_amount_in_lacs = s['stateBean']['stateTotalAmount']
                                                                        except:
                                                                            outstanding_amount_in_lacs = ""
                                                                        item = {}
                                                                        item['uid'] = ""
                                                                        item['name'] = name
                                                                        item['alias_name'] = []
                                                                        item['country'] = []
                                                                        item['list_type'] = "Individual"
                                                                        item['last_updated'] = ""
                                                                        item['individual_details'] = {}
                                                                        item['sanction_details'] = {}
                                                                        item['sanction_details']['Date'] = date
                                                                        item['sanction_details']['outstanding_amount_in_lacs'] = outstanding_amount_in_lacs
                                                                        item['sanction_details']['Borrow_Branch_Name'] = branch_name
                                                                        item['sanction_details']['Borrow_Bank_Name'] = bank_name
                                                                        item['family_tree'] = {}
                                                                        item['family_tree']['Associate'] = Director_name_and_Details
                                                                        item['nns_status'] = False
                                                                        item['address'] = []
                                                                        add = {}
                                                                        add['complete_address'] = address
                                                                        add['country'] = "India"
                                                                        item['address'].append(add)
                                                                        item['documents'] = {}
                                                                        item['comment'] = ""
                                                                        item['sanction_list'] = {}
                                                                        item['sanction_list']['sl_authority'] = "Reserve Bank of India, India"
                                                                        item['sanction_list']['sl_url'] = "https://suit.cibil.com/loadSuitFiledDataSearchAction"
                                                                        item['sanction_list']['sl_host_country'] = "India"
                                                                        item['sanction_list']['sl_type'] = "Sanctions"
                                                                        item['sanction_list']['watch_list'] = "India Watchlists"
                                                                        item['sanction_list']['sl_source'] = "Reserve Bank of India Willful Defaulters List, India"
                                                                        item['sanction_list']['sl_description'] = "List of Will full defaulters issued by Reserve Bank of India and sourced from CIBIL Suit Filed Accounts."
                                                                        item['sanction_list']['list_id'] = "IND_E20010"
                                                                        self.final_list.append(item)
                                                            except Exception as e:
                                                                print(e)
                                                    except Exception as e:
                                                        print(e)
                                                except Exception as e:
                                                    print(e)
                                            except Exception as e:
                                                print(e)

                                    except:
                                        body = ""
                                except:
                                    json_data = ""

                            except Exception as e:
                                print(e)
                    except Exception as e:
                        print(e)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)
    def csv(self):
        try:
            ab = json.dumps(self.final_list)
            f = open(f"{self.outputpath}/{self.date}_output_suit.cibil.com_744.json", "w")
            f.write(ab)
            f.close()
        except Exception as e:
            print(e)
temp = Data()
# temp.parsing()
temp.parsing1()
temp.csv()