import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
# import hashlib

class AuEntitySpy(CrawlSpider):
    name = 'auspy'
    allowed_domains = ['moneysmart.gov.au']
    start_urls = ['https://moneysmart.gov.au/companies-you-should-not-deal-with']
    custom_settings = {"FEED_EXPORT_ENCODING" : "utf-8"}


    rules = (
        Rule(LinkExtractor(restrict_xpaths="//div[@class='accordianRowContent']/div/h4/a"), callback='parse_item', follow=True),
    )

    # def get_hash(n):
    #     return hashlib.sha256(((n+"Prohibited Investment List").lower()).encode()).hexdigest() 

    def parse_item(self, response):
        name = response.xpath("//div[@id='heroText']//h1/text()").get()
        data = response.xpath("//div[@class='cell'][2]/p/text()").getall()
        adrs = []
        phone = []
        mail = []
        website = []
        for i in data:
            if "Address:" in  i:
                ad = i.split("Address:")[-1]
                if ad:
                    adrs.append(ad)

            elif "Ph:" in i:
                ph = i.split("Ph:")[-1]
                if ph:
                    phone.append(ph)
            elif "Fax:" in i:
                ph = i.split("Fax:")[-1]
                if ph:
                    phone.append(ph)

            elif "Email:" in i:
                m = i.split("Email:")[-1]
                if m:
                   mail.append(m)  
        
            elif "Website:" in i:
                w = i.split("Website:")[-1]
                if w:
                    website.append(w)

        yield{
            "name" : name,
            "address" : adrs,
            "phone" : phone,
            "mail" : mail,
            "web" : website
        }
        # name = response.xpath("normalize-space(//div[@id='product-title']/h1/text())").get()
        # descp = response.xpath("normalize-space((//article[@id='descriptions']//p)[1])").get()
        # price = response.xpath("normalize-space(//p[@class='price']/span[1]/text())").get()
        # brand = response.xpath("normalize-space(//div[@id='product-subtitle']/a/span/text())").get()
        # brand_url = response.xpath("//div[@id='product-subtitle']/a/@href").get()
        # ingredients  = response.xpath("normalize-space(//article[@id='Nutritional-Info']//p[1])").get()
        # key_benefits = response.xpath("(//article[@id='descriptions']//ul)[1]/li/text()").getall()
        # imgs = response.xpath("//div[@id='media-selector']//img/@src").getall()

        # attributes = {}
        # for attrib in response.xpath("//ul[@class='attributes']/li"):
        #     attrib_name = attrib.xpath("normalize-space(.//div[1]/text())").get()
        #     if attrib.xpath("normalize-space(.//div[2]/text())").get():
        #         attrib_value = attrib.xpath("normalize-space(.//div[2]/text())").get()
        #     else:
        #         attrib_value = attrib.xpath("normalize-space(.//div[2]/span/text())").get()

        #     attributes[attrib_name] = attrib_value

        # analysis = {}
        # for data in response.xpath("(//table)[1]/tbody/tr"):
        #     data_name = data.xpath("normalize-space(.//td[1])").get()
        #     data_value = data.xpath("normalize-space(.//td[2])").get()
        #     analysis[data_name] = data_value

        # yield{
        #     "ProductName" : name,
        #     "Price" : price,
        #     "Description" : descp,
        #     "Attributes" : attributes,
        #     "Brand" : brand,
        #     "Brand_url": brand_url,
        #     "ingredients" : ingredients,
        #     "Key Benefits" : key_benefits,
        #     "Guaranteed Analysis" : analysis,
        #     "Images" : imgs

        # }