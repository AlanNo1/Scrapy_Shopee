# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


# shopee网站爬虫
class ParentItem(scrapy.Item):
    # define the fields for your item here like:
    catid = scrapy.Field()
    display_name = scrapy.Field()
    second_url = scrapy.Field()
    child_cat_list = scrapy.Field()
    second_cage_catid = scrapy.Field()
    second_cage_display_name = scrapy.Field()


class ChildItem(scrapy.Item):
    # define the fields for your item here like:
    child_detail_url = scrapy.Field()
    child_catid = scrapy.Field()
    child_display_name = scrapy.Field()
    child_images = scrapy.Field()
    child_price = scrapy.Field()
    child_historical_sold = scrapy.Field()
    # 类别
    display_name = scrapy.Field()
    catid = scrapy.Field()
