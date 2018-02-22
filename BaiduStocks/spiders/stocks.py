# -*- coding: utf-8 -*-
import scrapy
import re
import pymysql


class StocksSpider(scrapy.Spider):
    name = 'stocks'    
    start_urls = ['http://quote.eastmoney.com/stocklist.html']

    def parse(self, response):
        if (False): # stocks info table
            self.con = pymysql.Connect(host='127.0.0.1', user='root', passwd='123456', database='Stocks', charset='utf8')
            self.cu = self.con.cursor()
            tableName = 'stockInfo'
            sql =  """CREATE TABLE IF NOT EXISTS {}( Code VARCHAR(10) PRIMARY KEY, 股票名称 VARCHAR(10), 交易所 VARCHAR(10), 板块1 VARCHAR(10), 板块2 VARCHAR(10), 板块3 VARCHAR(10), 题材1 VARCHAR(10), 题材2 VARCHAR(10), 题材3 VARCHAR(10))""".format(tableName)
            self.cu.execute(sql)
            self.con.commit()
            
            for href in response.css('a::attr(href)').extract():
                try:
                    stock = re.findall(r"[s][hz]\d{6}", href)[0]                                
                    sql = """INSERT INTO {}(Code) VALUES ('%s')""".format(tableName) %(stock)
                    self.cu.execute(sql)
                    self.con.commit()              
                except:
                    continue
            self.con.close()

        for href in response.css('a::attr(href)').extract():
            try:
                stock = re.findall(r"[s][hz]\d{6}", href)[0]
                stockURL = 'https://gupiao.baidu.com/stock/'
                url = stockURL + stock + '.html'
                yield scrapy.Request(url, callback=self.parse_stock)
            except:
                continue

    def parse_stock(self, response):
        infoDict = {}
        stockInfo = response.css('.stock-bets')
        name = stockInfo.css('.bets-name').extract()[0]
        datestr = stockInfo.css('.f-up').extract()[0]
        datestr = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', datestr)[0]
        keyList = stockInfo.css('dt').extract()
        valueList = stockInfo.css('dd').extract()

        for i in range(len(keyList)):
            key = re.findall(r'>.*</dt>', keyList[i])[0][1:-5]
            try:
                val = re.findall(r'\d+\.?.*</dd>', valueList[i])[0][0:-5]
            except:
                val = '0.0'
            infoDict[key] = val


        infoDict.update(
            {'股票名称': re.findall('\s.*\(', name)[0].split()[0],
            'code': re.findall('\>.*\<', name)[0][1:-1],
            '日期': datestr})        
        yield infoDict


