# -*-coding:utf-8-*-
from scrapy import Request
from scrapy.spiders import Spider  # 导入Spider类
from shopee.items import ParentItem  # 导入模块
from shopee.items import ChildItem  # 导入模块


class HotSalesSpider(Spider):
    # 定义爬虫名称
    name = 'shopee'

    # 获取初始Request
    def start_requests(self):
        url = "https://sg.xiapibuy.com/api/v4/pages/get_category_tree"
        # 生成请求对象，设置url，headers，callback
        yield Request(url, callback=self.parse_category)

    # 解析category数据
    def parse_category(self, response):
        main_detail_response = response.json()['data']['category_list']
        for category_list in main_detail_response:
            child_cat_list = []
            # 分类信息
            item = ParentItem()
            item['catid'] = category_list['catid']
            item['display_name'] = category_list['display_name']
            # item['second_url'] = f"https://sg.xiapibuy.com/api/v4/recommend/recommend?bundle=category_landing_page&catid={category_list['catid']}&limit=60&offset="
            for child_catid in category_list['children']:
                item['second_cage_catid'] = child_catid['catid']
                item['second_cage_display_name'] = child_catid['display_name']
                item[
                    'second_url'] = f"https://sg.xiapibuy.com/api/v4/recommend/recommend?bundle=category_landing_page&catid={child_catid['catid']}&limit=60&offset="
                # item['child_cat_list'] = child_cat_list
                # 获取下一页URL，并生成Request请求，提交给引擎
                # 1.获取下一页URL
                current_page = 0  # 设置当前页，起始为0
                while current_page <= 1000:
                    current_page += 1
                    next_url = f"{item['second_url']}{current_page}"
                    print(f"正在爬取类别{next_url}")
                    # 2.根据URL生成Request，使用yield返回给引擎
                    yield Request(url=next_url, callback=self.parse_product, meta={'item': item})

    # 解析product的数据
    def parse_product(self, response):
        parent_item = response.meta['item']
        product_list = response.json()['data']['sections'][0]['data']['item']
        for product in product_list:
            item = ChildItem()
            item['catid'] = parent_item['catid']
            item['display_name'] = parent_item['display_name']
            item['child_catid'] = product['catid']
            item['child_display_name'] = product['name']
            item[
                'child_detail_url'] = f"https://sg.xiapibuy.com/{product['name']}s-i.{product['shopid']}.{product['itemid']}"
            item['child_images'] = ','.join(
                ['https://cf.shopee.sg/file/' + images_url for images_url in product['images']])
            item['child_price'] = int(product['price'] / 1000)
            item['child_historical_sold'] = product['historical_sold']
            yield item
