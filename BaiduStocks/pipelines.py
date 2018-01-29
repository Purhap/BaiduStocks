# -*- coding: utf-8 -*-
import time
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class BaidustocksPipeline(object):
    def process_item(self, item, spider):
        return item

class BaidustocksInfoPipeline(object):
    def process_item(self, item, spider):
    	try:
    		line = str(dict(item)) + '\n'
    		self.f.write(line)    	
    	except:
    		pass

    	return item

    def open_spider(self, spider):
        output_file = 'D://tmp//StockData//BaiduStockInfo' + time.strftime("%d-%m-%Y") + '.txt'
       	self.f = open(output_file, 'w')

    def close_spider(self, spider):
    	self.f.close()



		