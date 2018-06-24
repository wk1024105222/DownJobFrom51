# coding:utf-8
"""本模块 包含任务过程中 涉及的所有步骤"""
import os
import re
import urllib
import logging
import time
from crawler import base
from crawler.dbpool import pool
from crawler.stock.util import getStockListeddate, getSinaSeasonUrl, getPriceFromHtml
from entity import *

STOCKBASEPATH = 'D:/fileloc/stock/price'

EASTMONEY = ['http://quote.eastmoney.com/stocklist.html',
         re.compile(r'<li><a target="_blank" href="http://quote.eastmoney.com/[a-z]{2}\d{6}.html">'
                    r'(.*?)\(([6,0,3]\d{5})\)</a></li>')]


class GetAllListedStock(base.BaseThread):
    """
    从东方财富网获取所有已发行的股票号码 以及名称
    数据库入库不校验是否存在
    """
    def run(self):
        url = EASTMONEY[0]
        stockCodeReg = EASTMONEY[1]

        pageHtml = urllib.urlopen(url).read()
        stockCodes = re.findall(stockCodeReg, pageHtml)

        for tmp in stockCodes:
            if (len(tmp) == 2):
                code = tmp[1]
                name = tmp[0]

                if self.doneMap.has_key(code):
                    if self.doneMap[code] != name:
                        self.dbQueue.put("update stockinfo set name ='%s' where code ='%s'" % (name, code))
                    continue
                self.outQueue.put(code)
                self.dbQueue.put(StockInfo(code=code, name=name).createInsertSql())
        return

    @staticmethod
    def fillInQueue(inQueue):
        pass

    @staticmethod
    def fillDoneQueue(doneQueue):
        con = pool.connection()
        cursor = con.cursor()
        cursor.execute("select code, name from STOCKINFO")
        for stock in cursor.fetchall():
            doneQueue[stock[0]] = stock[1]
        con.close()

class UpdateStockInfo(base.BaseThread):
    """
    更新股票各项信息 目前只更新上市时间
    """
    def run(self):
        emptyNum=0
        while True:
            if self.inQueue.empty():
                if emptyNum>10:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                else :
                    emptyNum+=1
                    logging.info('UpdateStockInfo inQueue is empty wait for '+str(self.emptyWait)+'s')
                    time.sleep(self.emptyWait)
                    continue
            # 计数归零
            emptyNum=0

            code = None
            try:
                code = self.inQueue.get()
            except Exception as e:
                logging.error(e)

            if code == None:
                logging.info('UpdateStockInfo get null task from inQueue wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue

            listeddate = getStockListeddate(code)
            if listeddate != None:
                sql = "update stockinfo set listeddate='%s' where code='%s'" % (listeddate,code)
                self.dbQueue.put(sql)
                self.outQueue.put(code)
            time.sleep(self.requestWait)
        return

    @staticmethod
    def fillInQueue(inQueue):
        con = pool.connection()
        cursor = con.cursor()
        cursor.execute("select code from STOCKINFO where LISTEDDATE is not null")
        for code in cursor.fetchall():
            inQueue.put(code[0])
        con.close()

    @staticmethod
    def fillDoneQueue(doneQueue):
        return

class GetLackPriceHtmlUrl(base.BaseThread):
    """
    生成下载缺失数据 html 的url
    """
    def run(self):
        emptyNum=0
        while True:
            if self.inQueue.empty():
                if emptyNum>10:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                else :
                    emptyNum+=1
                    logging.info('DownLackPriceFile inQueue is empty wait for '+str(self.emptyWait)+'s')
                    time.sleep(self.emptyWait)
                    continue
            # 计数归零
            emptyNum=0

            task = None
            try:
                task = self.inQueue.get()
            except Exception as e:
                logging.error(e)

            if task == None:
                logging.info('DownLackPriceFile get null task from inQueue wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue
            code = task[0]
            urls = getSinaSeasonUrl(task)

            for url in urls:
                self.outQueue.put((code,url))

            time.sleep(self.requestWait)
        return

    @staticmethod
    def fillInQueue(inQueue):
        con = pool.connection()
        cursor = con.cursor()
        cursor.execute("select code, max(txndate) from STOCKINFO group by code")
        for stock in cursor.fetchall():
            inQueue.put(stock)
        con.close()

    @staticmethod
    def fillDoneQueue(doneQueue):
        return

class DownLackPriceHtml(base.BaseThread):
    """
    下载缺失股价 的html文件
    """
    def run(self):
        while True:
            if self.inQueue.empty():
                logging.info('DownLackPriceHtml inQueue is empty wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)

            task = None
            try:
                task = self.inQueue.get()
            except Exception as e:
                logging.error(e)
                continue

            if task == None:
                logging.info('DownLackPriceHtml get null from inQueue wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue

            code = task[0]
            url = task[1]

            filepath = '%s/%s_%s_%s.html' % (STOCKBASEPATH,code,url[-11:-7],url[-1])

            #删除已存在文件 文件中可能包含不完整的季度数据
            if not os.path.exists(filepath):
                os.remove(filepath)
                logging.info('exist file %s is deleted' % (filepath))

            try:
                urllib.urlretrieve(url,filepath+".tmp")
                os.renames(filepath+".tmp",filepath)
                self.outQueue.put((code,filepath))
                logging.info('[%s] down success url:%s' % (filepath.split('/')[-1],url))
            except Exception as e:
                logging.error(e)
                logging.error('[%s] down failed url:%s' % (filepath.split('/')[-1],url))
            time.sleep(self.requestWait)
        return

    @staticmethod
    def fillInQueue(inQueue):
        pass

    @staticmethod
    def fillDoneQueue(doneQueue):
        pass

class AnalysisLackPriceHtml(base.BaseThread):
    """
    解析包含股价信息的html
    """
    def run(self):
        emptyNum=0
        con = pool.connection()
        cursor = con.cursor()
        while True:
            if self.inQueue.empty():
                if emptyNum>10:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                else :
                    emptyNum+=1
                    logging.info('AnalysisLackPriceHtml inQueue is empty wait for '+str(self.emptyWait)+'s')
                    time.sleep(self.emptyWait)
                    continue
            # 计数归零
            emptyNum=0

            task = None
            try:
                task = self.inQueue.get()
            except Exception as e:
                logging.error(e)

            if task == None:
                logging.info('AnalysisLackPriceHtml get null task from inQueue wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue

            filepath = task[1]

            if not os.path.exists(filepath):
                os.renames(filepath, filepath + '.err')
                logging.info('file %s not exists' % (filepath))
                continue

            html = open(filepath).read()
            prices = getPriceFromHtml(html)
            if len(prices) == 0:
                logging.info("analysisHtml error len=0")
                continue

            try:
                for price in prices:
                    sql = price.createInsertSql()
                    cursor.execute(sql)
                con.commit()
                os.renames(filepath, filepath + '.done')
            except Exception as e:
                logging.error(e)
                con.rollback()
                logging.error("analysisHtml save DB error")
        con.close()
        return

    @staticmethod
    def fillInQueue(inQueue):
        for file in os.listdir(STOCKBASEPATH):
            if file.endswith('html'):
                filepath = '%s/%s' % (STOCKBASEPATH,file)
                inQueue.put(('',filepath))

    @staticmethod
    def fillDoneQueue(doneQueue):
        return

class DBExecuter(base.BaseThread):
    """
    数据入库
    """
    def run(self):
        con = pool.connection()
        cursor = con.cursor()
        emptyNum=0
        # fp = open("allsql.script","w")
        while True:
            if self.inQueue.empty():
                if emptyNum>50:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                else:
                    emptyNum+=1
                    logging.info('SaveJobInfoToDB inQueue is empty wait for '+str(self.emptyWait)+'s emptyNum:'+str(emptyNum))
                    time.sleep(self.emptyWait)
                    continue
            emptyNum=0

            sql = None
            try:
                sql = self.inQueue.get()
            except Exception as e:
                logging.error(e)

            if sql == None:
                logging.info('SaveJobInfoToDB get null task from inQueue wait for '+str(self.emptyWait)+'s emptyNum:'+str(emptyNum))
                time.sleep(self.emptyWait)
                continue

            try:
                cursor.execute(sql.encode('gb2312','ignore'))
                con.commit()
            except Exception as e:
                logging.info(sql)
                logging.info(e)
        con.close()
        logging.info('thread is over')
        return

    @staticmethod
    def fillInQueue(inQueue):
        return

    @staticmethod
    def fillDoneQueue(doneQueue):
        return
