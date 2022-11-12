import requests
import json
import pymysql
import time
from flask import Flask, jsonify
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from datetime import datetime

# 清除SSL认证日志
requests.packages.urllib3.disable_warnings()
# 实例化app
app = Flask(import_name=__name__)

headers = {
    'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.26',
    'af-ac-enc-dat': "AAcyLjQuMS0yAAABhDjGgcUAAAtdAkgAAAAAAAAAAOYyhFAbVQMMpIKa2+dGIBkKaWUVkWOzjLDykZY2dhCO2aemll8p6VxbW7tbRdHQS130i7q8HZ2GTC6Gy8upKta1AomlahQQzJTI3QAyabrcR+HjRB1nCnAsDVuG+2obETHDZK7Q5PH3Xqg0882frwuCRe0OFRBxyjGBDeefPQSexL2ukBJiSuxjXhGU6mX4nJbWXvbTXy03qy5yS9j2Ey/90PcDmVG2rUT03+NpNuFFxwWbIq+H8FsUC1SsOYA7uQssm6UlVhQnezi7GN9B49To0imbwzU6ZAIWKr3Tsiwkxq+NZB8hRld2WqENNTV9bukfQJwRul/bNcp2Sur50wYMwSDikzRUuI9VU/LDpdQRLTm64vGVFtzkzh3gjuLxgwquIESRZr8EptQTXXHI8QHpIBMd8cK6D+mS+izuW4fUEsbihmJFJkfOAnBywenZjD51w1BpGoHGvTBYivEZkbKzdr3CwzU6ZAIWKr3Tsiwkxq+NZGkuLNAsarAeTcFnp+9U8DP6t8iT3JFX5LAEVsOOieB3NvNHCQwqdAFStQ7FHc/t0BfvIaXng5DA5dFl5DxycVn7tU3nydm0ResOY1uboiy9vTQWDxlJKdweeCmwNlmjFRC3q7r7Amu/FcS/fiwQIrIU8rtCD6OwJVzKZGrIiNhpeYiR9LCsMLBIQutXUWm6mLxuJP9sXZDPfE67TbCFml1Z+JsTQD8A8KWyAtyS6tB8kWOzjLDykZY2dhCO2aemlivQ0YIeaYrmq7QPviyd+aOq1Xz8aRCetoCArERrz7Gc"
}
# 分类类别页
main_detail_url = 'https://sg.xiapibuy.com/api/v4/pages/get_category_tree'
# 分类详情页
second_url = f'https://sg.xiapibuy.com/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid=11012819&limit=60&offset='


# 获取主分类信息
def get_main_cag():
    print("开始爬取主页信息")
    start_time = time.time()
    main_list = []
    child_cat_list = []
    main_detail_response = requests.get(main_detail_url, headers=headers, verify=False).json()['data']['category_list']
    for category_list in main_detail_response:
        # 分类信息
        main_dict = {}
        main_dict['catid'] = category_list['catid']
        main_dict['display_name'] = category_list['display_name']
        main_dict[
            'second_url'] = f"https://sg.xiapibuy.com/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid={category_list['catid']}&limit=60&offset="
        for child_catid in category_list['children']:
            category_dict = {}
            category_dict['child_catid'] = child_catid['catid']
            category_dict['display_name'] = child_catid['display_name']
            child_cat_list.append(category_dict)
        main_dict['child_cat_list'] = child_cat_list
        main_list.append(main_dict)
    print(f"爬取主页{main_detail_url}信息完毕，共耗时{time.time() - start_time}")
    return main_list


# 获取详细类信息
def get_second_cag(main_list, page):
    print("开始爬取详细页信息")
    start_time = time.time()
    seconnd_list = []
    for main_dict in main_list:
        second_url = f"{main_dict['second_url']}{page}"
        second_response_detail = \
            requests.get(second_url, headers=headers, verify=False).json()['data']['sections'][0]['data']['item']
        for second in second_response_detail:
            # 分类详情页信息
            seconnd_dict = {}
            seconnd_dict['商品大类'] = main_dict['display_name']
            seconnd_dict['商品大类ID'] = main_dict['catid']
            seconnd_dict['商品二类ID'] = second['catid']
            seconnd_dict['商品名称'] = second['name']
            seconnd_dict[
                '商品详情页链接'] = f"https://sg.xiapibuy.com/{second['name']}s-i.{second['shopid']}.{second['itemid']}"
            seconnd_dict['商品轮播图'] = str(['https://cf.shopee.sg/file/' + images_url for images_url in second['images']])
            seconnd_dict['价格(美元)'] = round(int(second['price']) / 100000, 3)
            seconnd_dict['已售数量'] = second['historical_sold']
            seconnd_list.append(seconnd_dict)
        print(f"爬取详情页{second_url}信息完毕，共耗时{time.time() - start_time},爬取第{len(seconnd_list)}个商品")
    return seconnd_list


def save_spider_json(seconnd_list):
    with open(f'xiapibuy{time.time()}.json', mode='w', encoding='utf-8') as f:
        json.dump(seconnd_list, f, ensure_ascii=False)
        print("最新爬虫数据已经备份，正在存储在数据库")


def save_spider(all_list):
    # 连接数据库
    db = pymysql.connect(host="152.32.128.199", user="spider", password="132302007", database="spider",
                         charset="utf8mb4")
    cursor = db.cursor()
    for seconnd_dict in all_list:
        # 获取游标
        # 执行SQL语句组装
        # updatesql = "insert into xiapibuy (cat_type,cat_name,second_type_id,detail_url, images_url, price, historical_sold) " \
        #             "values (%s,%s,%s,%s,%s,%s,%s)"
        insertsql1 = "insert into product (title,images,price,sold_num, detail_url,status,category_name,created_at) " \
                     "values (%s,%s,%s,%s,%s,%s,%s,%s)"
        insertsql2 = "insert into category (pid,name,status,created_at) " \
                     "values (%s,%s,%s,%s)"
        selectsql = "select id from product where title=%s and images=%s and price=%s and sold_num=%s and " \
                    "detail_url=%s and category_name=%s"
        cat_id = seconnd_dict['商品大类ID']
        cat_type = seconnd_dict['商品大类']
        cat_name = seconnd_dict['商品名称']
        second_type_id = seconnd_dict['商品二类ID']
        detail_url = seconnd_dict['商品详情页链接']
        images_url = ','.join(seconnd_dict['商品轮播图'])
        price = int(seconnd_dict['价格(美元)'] * 1000)
        historical_sold = seconnd_dict['已售数量']
        print(cat_type, cat_name, second_type_id, detail_url, images_url, price, historical_sold)
        # 执行数据库
        cursor.execute(selectsql,
                       (cat_name, images_url, price, historical_sold, detail_url, cat_type))
        results = cursor.fetchone()
        if results:
            print(f"商品{seconnd_dict['商品名称']},地址{seconnd_dict['商品详情页链接']}暂无更新")
        else:
            try:
                cursor.execute(insertsql1,
                               (cat_name, images_url, price, historical_sold, detail_url, 0, cat_type,
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                cursor.execute(insertsql2,
                               (cat_id, cat_name, 0, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                # 提交更新
                db.commit()
                print(f"商品{seconnd_dict['商品名称']},地址{seconnd_dict['商品详情页链接']}已经保存成功")
            except:
                # 回滚
                db.rollback()
                print(f"商品{seconnd_dict['商品名称']},地址{seconnd_dict['商品详情页链接']}暂无更新")
    cursor.close()
    db.close()


# @app.route('/<int:page>/', methods=["GET", "POST"])
def shopee_main(page):
    main_list = get_main_cag()
    with ThreadPoolExecutor(max_workers=8) as pool:
        par = partial(get_second_cag, main_list)
        results = pool.map(par, (i for i in range(page)))
        for result in results:
            save_spider_json(result)
            # 数据库保存信息
            save_spider(result)
            # return jsonify(all_list)


if __name__ == '__main__':
    # 启动服务:http://127.0.0.1:5000/page(传入爬取的页面数量)
    # app.run()
    shopee_main(10)
