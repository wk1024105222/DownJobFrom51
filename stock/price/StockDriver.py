# encoding: utf-8
from datetime import datetime, time
import logging
import threading
import socket
from Queue import Queue
import os
import os.path

from stock.dbpool import pool
from stock.StockUtil import source2
from stock import StockUtil


socket.setdefaulttimeout(5.0)

logging.basicConfig(level=logging.INFO,
                filemode='w',
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/StockDriver.log')

lock = threading.Lock()

def updateStockCode():
    """
    从网上获取最新股票列表 更新数据库
    :param source: 数据源 List [url,获取code的reg]
    :return:
    """

    util = StockUtil()
    dbStocks = util.getAllStockInfoFromDB('Dict')
    webStocks = util.getAllStockInfoFromWeb(source2)

    #con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai")
    con = pool.connection()
    cursor = con.cursor()

    for stockInfo in webStocks:
        code = stockInfo.code
        name = stockInfo.name

        if  not code in dbStocks:
            sql = "insert into STOCKINFO (CODE, NAME)  values ('"+code+"', '"+name+"')"
            logging.info((code,name,'doing insert'))
        elif name != dbStocks[code][1]:
            sql = "update stockinfo set name='"+name+"' where code='"+code+"'"
            logging.info((code,name,'doing update name'))
        else:
            continue
        try:
            cursor.execute(sql)
            con.commit()
            logging.info((code,name,'save success'))
        except Exception as e:
            logging.error(e)
            logging.error((code,name,'save failed'))
    con.close()

def updateAllStocksListeddate():
    """
    获取新股票的上市时间  更新数据库 通过sina股票信息页面
    http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpInfo/stockid/600115.phtml
    :return:
    """

    util = StockUtil()

    allStock = util.getAllStockInfoFromDB('Dict')

    #con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai")
    con = pool.connection()
    cursor = con.cursor()

    for code in allStock.keys():
        if allStock[code][0] != None:
            listeddate = datetime.strftime(allStock[code][0],'%Y-%m-%d')
        else:
            listeddate=None

        newDate = util.getStockListeddate(code)
        if newDate != None:
            print newDate
            if newDate == listeddate:
                print code+"上市时间"+newDate+"无异常"
            else:
                sql = "update stockinfo set listeddate=to_date('"+newDate+"','yyyy-mm-dd') where code='"+code+"'"
                print sql
                cursor.execute(sql)
                con.commit()
        else:
            print code+"未找到上市时间"
    con.close()

def aaa(stock):
    util = StockUtil()
    if stock != None:
        #con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai")
        con = pool.connection()
        cursor = con.cursor()

        if stock['listeddate'] != None:
            listeddate = datetime.strftime(stock['listeddate'],'%Y-%m-%d')
        else:
            listeddate=None

        newDate = util.getStockListeddate(stock['code'])
        if newDate != None:
            #print "new date="+newDate
            if newDate == listeddate:
                print stock['code']+"上市时间"+newDate+"无异常"
            else:
                sql = "update stockinfo set listeddate=to_date('"+newDate+"','yyyy-mm-dd') where code='"+stock['code']+"'"
                #print script
                cursor.execute(sql)
                con.commit()
        else:
            print stock['code']+"未找到上市时间"
        con.close()
    else:
        return

class DownHtml(threading.Thread):
    def run(self):
        global queue
        util = StockUtil()
        while True:
            stock = queue.get()
            if stock != None:
                util.downLackPriceFromSinaBySeason(stock['code'], stock['listeddate'])
            else:
                return


def analysisHtml(filename):
    logging.info("begin analysisHtml "+ filename)
    util = StockUtil(filename[-18:-12])
    html = open(filename).read()
    prices = util.getPriceFromHtml(html)
    if len(prices) == 0:
         logging.info("analysisHtml error len=0")
    con = pool.connection()
    cursor = con.cursor()
    try:
        for price in prices:
            sql = price.createInsertSql()
            cursor.execute(sql)
        con.commit()
    except Exception as e:
        logging.info(str(e))
        con.rollback()
        con.close()
        logging.info("analysisHtml save DB error")
        return
    con.close()
    # os.renames(filename,filename+'.done')
    logging.info("analysisHtml finish")

class AnalysisHtml(threading.Thread):
    def run(self):
        global queue
        while True:
            if lock.acquire():
                filename = queue.get()
                lock.release()
                analysisHtml(filename)
            else:
                time.sleep(2)

def processDirectory ( args, dirname, filenames ):
    global queue
    for filename in filenames:
        if filename.endswith('html'):
            queue.put(dirname+"\\"+filename)

def rename ( args, dirname, filenames ):
    for filename in filenames:
        if filename.endswith('done'):
            old = dirname+"\\"+filename
            new = old[0:-5]
            print old ,new
            #os.renames(filename,filename+'.done')

class updateStockListeddate(threading.Thread):

    def __init__(self,tmp):
        super(updateStockListeddate, self).__init__()
        self.methon=tmp
        print tmp
    def run(self):
        global queue
        util = StockUtil()
        while True:
            if lock.acquire():
                stock = queue.get()
                lock.release()
                print queue.qsize()
                aaa(stock)
            else:
                 time.sleep(2)

if __name__=='__main__':
    queue = Queue()

    """
    util = StockUtil()
    allStock = util.getAllStockInfoFromDB('List')
    for stock in allStock:
        queue.put(stock)
    print queue.qsize()

    """
    os.path.walk(r'v:\stock', processDirectory, None )
    print queue.qsize()
    threads = []
    for i in range(1):
        thread = AnalysisHtml()
        threads.append(thread)
        thread.start()
    for a in threads:
        a.join()






