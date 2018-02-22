# -*- coding: utf-8 -*-
import time
import pymysql
import decimal
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class BaidustocksInfoPipeline(object):
    def open_spider(self, spider):
        self.con = pymysql.Connect(host='127.0.0.1', user='root', passwd='123456', database='Stocks', charset='utf8')
        self.cu = self.con.cursor()

    def spider_close(self, spider):
        self.con.close()

    def process_item(self, item, spider):
        try:        
            tableName = item['股票名称'] +'_'+ item['code']
            sql =  """CREATE TABLE IF NOT EXISTS {}( 日期 DATE PRIMARY KEY, 成交量 VARCHAR(10), 今开 REAL, 最高 REAL, 涨停 REAL, 内盘 VARCHAR(10), 成交额 VARCHAR(10), 委比 VARCHAR(10), 流通市值 VARCHAR(10), 市盈率MRQ VARCHAR(10),  每股收益 REAL, 总股本 VARCHAR(10), 昨收 REAL, 换手率 VARCHAR(10), 最低 REAL, 跌停 REAL, 外盘 VARCHAR(10), 振幅 VARCHAR(10),  量比 VARCHAR(10), 总市值 VARCHAR(10), 市净率 REAL,  每股净资产 REAL, 流通股本 VARCHAR(10))""".format(tableName)
        
            self.cu.execute(sql)
            self.con.commit()
            StocksInfoTable = 'stockInfo'
            stockCode = item['code']
            stockCode =  '%'+ stockCode   
            sql =  """SELECT Code, 股票名称 FROM {} WHERE Code LIKE '%s' """.format(StocksInfoTable) %(stockCode)
            self.cu.execute(sql)
            retVal = self.cu.fetchone()
            stockCode = retVal[0]
            stockName = retVal[1]
            tradePlace = stockCode[0:2]
            if(stockName !=item['股票名称']):
                sql =  """UPDATE {} SET 股票名称 = '%s', 交易所 = '%s' WHERE Code = '%s' """.format(StocksInfoTable) %(item['股票名称'], tradePlace, stockCode)
                self.cu.execute(sql)
                self.con.commit()

            sql = """INSERT INTO {}(日期, 成交量, 今开, 最高, 涨停, 内盘, 成交额, 委比, 流通市值, 市盈率MRQ, 每股收益, 总股本, 昨收, 换手率, 最低, 跌停, 外盘, 振幅, 量比, 总市值, 市净率, 每股净资产, 流通股本 ) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')""".format(tableName) %(item['日期'], item['成交量'], item['今开'], item['最高'], item['涨停'], item['内盘'], item['成交额'], item['委比'], item['流通市值'], item['市盈率<sup>MRQ</sup>'], item['每股收益'], item['总股本'], item['昨收'], item['换手率'], item['最低'], item['跌停'], item['外盘'], item['振幅'], item['量比'], item['总市值'], item['市净率'], item['每股净资产'], item['流通股本'])

            self.cu.execute(sql)
            self.con.commit()
        except:
            pass
            
        return item


    

#class BaidustocksInfoPipeline(object):
    # def process_item(self, item, spider):
    # 	try:
    # 		line = str(dict(item)) + '\n'
    # 		self.f.write(line)    	
    # 	except:
    # 		pass

    # 	return item

    # def open_spider(self, spider):
    #     output_file = 'D://tmp//StockData//BaiduStockInfo' + time.strftime("%d-%m-%Y") + '.txt'
    #    	self.f = open(output_file, 'w')

    # def close_spider(self, spider):
    # 	self.f.close()



		