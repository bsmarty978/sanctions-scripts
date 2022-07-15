from httplib2 import Response
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

class Sng780(CrawlSpider):
    name = 'sng780'
    allowed_domains = ['wanted.mvs.gov.ua']
    start_urls = ['https://wanted.mvs.gov.ua/searchperson']
    custom_settings = {
        "FEED_EXPORT_ENCODING" : "utf-8",
        # "DOWNLOAD_DELAY" : 0.25
        "CONCURRENT_REQUESTS_PER_IP":16,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True
        }


    rules = (
        Rule(LinkExtractor(restrict_xpaths="//div[@class='section-content']//div/a"), callback='parse_item', follow=True),
        Rule(LinkExtractor(restrict_xpaths="//li[last()]/a"), follow=True),
    )

    # def get_hash(n):
    #     return hashlib.sha256(((n+"Prohibited Investment List").lower()).encode()).hexdigest() 

    def parse_item(self, response):
        surname = response.xpath("(//div[@class='info-list']//div[@data='second'])[5]/text()").get("").strip()
        name = response.xpath("(//div[@class='info-list']//div[@data='second'])[6]/text()").get("").strip()
        midname = response.xpath("(//div[@class='info-list']//div[@data='second'])[7]/text()").get("").strip()
        fname = f'{name} {midname} {surname}'

        yield{
            "name" : fname,
        }