from scrapy import  cmdline

#执行小说热销榜的命令
cmdline.execute("scrapy crawl shopee".split())