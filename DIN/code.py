import encodings
from playwright.sync_api import sync_playwright
from scrapy.http import HtmlResponse
import time
import json


with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    url = "https://www.mca.gov.in/mcafoportal/enquireDIN.do"
    page.goto(url)
    print(page.title())
    page.click("#msgboxclose")
    time.sleep(3)

    page.fill("//input[@id='DIN']", '01489310')
    print('entered')

    page.click("//input[@type='submit']")
    time.sleep(3)

    resp = HtmlResponse("example.com",body=page.content(),encoding='utf-8')

    tr = resp.xpath("//table//tr/td/text())").getall()
    print(tr)

