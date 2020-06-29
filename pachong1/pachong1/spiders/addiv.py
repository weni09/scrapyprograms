# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from pachong1.items import Pachong1Item
import datetime

class AddivSpider(CrawlSpider):
    name = 'addiv'
    allowed_domains = ['www.stats.gov.cn']

    start_urls = ['http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/']

    lastyear = datetime.datetime.now().year - 1
    url_rule = r"http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/" + str(lastyear) + r"/((?:(?:\d{2}/){0,2}(?:\d{2,6}){0,1})|index).html"
    rules = (
        Rule(LinkExtractor(allow=url_rule),callback='parse_item',follow=True),
    )
    def parse_item(self, response):
        item = Pachong1Item()
        item['id'] = response.xpath('//*[@class="provincetr"]//a//@href | //*[@class="citytr" or @class="countytr" or @class="towntr"]/td[1]//text()').re(r'\d{2,12}')
        item['name'] = response.xpath('//*[@class="provincetr"]//a//text() | //*[@class="citytr" or @class="countytr" or @class="towntr"]/td[2]//text()').extract()

        yield item

