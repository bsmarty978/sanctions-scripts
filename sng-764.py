from email import header
import encodings
from platform import libc_ver
from django import urls
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import hashlib
import json
from lxml import etree
from scrapy.http import HtmlResponse


last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

url = "https://www.occ.treas.gov/news-events/newsroom/news-issuances-by-year/alerts/index-alerts.html"
#headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

data_tag = requests.get(url)

htmlcontent = data_tag.content
#print(htmlcontent)

soup = BeautifulSoup(htmlcontent,'html.parser')

correct_links = soup.find("div" ,{"class":"superflex-container2"}).find_all("a",href=True)
#print(correct_links)

links_with_text = [a['href'] for a in correct_links if a.text]
#print(links_with_text)

properlinks = [] 
for i in links_with_text:
    singlelink = "https://www.occ.treas.gov" + i
    properlinks.append(singlelink)

#print(properlinks)

mylist = []
for i in properlinks:
    d = {
        "Title" : "",
        "Source" : "www.occ.treas.gov",
        "publishedAt" : "",
        "URL" : "",
        "query" : "",
        "uid" : "",
        "Content" : ""
    }
    d["URL"] = i
    res = requests.get(i)
    # res_soup = BeautifulSoup(res.content,'html.parser')
    resp = HtmlResponse("example.com",body=res.text,encoding='utf=8')
    # print(res.xpath("//table"))
    for tr in resp.xpath("//table[@id='example_full']/tbody/tr"):
        title = tr.xpath("./td/a/text()").get()
        d["Title"] = title
        content_url = "https://www.occ.treas.gov"+ tr.xpath("./td/a/@href").get()

        resp_c = requests.get(content_url)

        result = HtmlResponse("example.com",body=resp_c.text,encoding='utf=8')
        content = "".join(result.xpath("//div[@class='occ-grid9-12 ctcol issuance-ct']//text()").getall())

        d["Content"]= content
        
        date =  tr.xpath("./td/text()").get()
        d["publishedAt"] = date

        id = tr.xpath("./td[2]/text()").get()

        d["uid"] = hashlib.sha256(((d["Title"] +d["publishedAt"]+"OCC Counterfeit list, USA"+id).lower()).encode()).hexdigest()


    try:
        if d["Title"]!="":
            mylist.append(d)
            print(d)
    except:
        pass

with open('w1.json', 'w', encoding="utf-8") as file:
   json.dump(mylist, file, ensure_ascii=False, indent=4)