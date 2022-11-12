# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
import pymysql
from datetime import datetime


class QidianHotPipeline(object):
    def process_item(self, item, spider):
        if item["child_detail_url"] == "":
            return f"商品{item['child_detail_url']}无需爬取"
        else:
            print(f"正在爬取商品{item['child_detail_url']}")
        return item


# 去除重复作者的Item Pipeline
class DuplicatesPipeline(object):
    def __init__(self):
        # 定义一个保存商品信息的集合，
        self.author_set = set()

    def process_item(self, item, spider):
        if item['child_detail_url'] in self.author_set:
            # 抛弃重复的Item项
            raise DropItem("查找到重复的商品链接: %s" % item)
        else:
            self.author_set.add(item['child_detail_url'])
        return item


class SaveToTxtPipeline(object):
    # 定义数据库
    def open_spider(self, spider):
        host = spider.settings.get("HOST", "152.32.128.199")
        user = spider.settings.get("USER")
        password = spider.settings.get("PWD")
        database = spider.settings.get("DB")
        self.db = pymysql.connect(host=host, user=user, password=password, database=database,
                                  charset="utf8mb4")
        self.cursor = self.db.cursor()

    # 数据处理
    def process_item(self, item, spider):
        insertsql1 = "insert into product (title,images,price,sold_num, detail_url,status,category_name,created_at) " \
                     "values (%s,%s,%s,%s,%s,%s,%s,%s)"
        insertsql2 = "insert into category (pid,name,status,created_at) " \
                     "values (%s,%s,%s,%s)"
        selectsql = "select id from product where title=%s and images=%s and price=%s and sold_num=%s and " \
                    "detail_url=%s and category_name=%s"
        # 执行数据库
        self.cursor.execute(selectsql,
                            (item['child_display_name'], item['child_images'], item['child_price'],
                             item['child_historical_sold'], item['child_detail_url'], item['display_name']))
        results = self.cursor.fetchone()
        if results:
            raise DropItem(f"商品{item['child_display_name']}暂无更新")
        else:
            try:
                self.cursor.execute(insertsql1,
                                    (item['child_display_name'], item['child_images'], item['child_price'],
                                     item['child_historical_sold'], item['child_detail_url'], 0, item['display_name'],
                                     datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                self.cursor.execute(insertsql2,
                                    (item['catid'], item['display_name'], 0,
                                     datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                # 提交更新
                self.db.commit()
                print(f"商品{item['child_display_name']}已经保存成功")
            except:
                # 回滚
                self.db.rollback()
                print(f"商品{item['child_display_name']}正在回滚")
        return item

    # Spider关闭时，执行关闭文件操作
    def close_spider(self, spider):
        # 关闭文件
        self.cursor.close()
        self.db.close()
