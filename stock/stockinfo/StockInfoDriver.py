# encoding: utf-8
from datetime import datetime, time
import logging
import threading
import socket
from stock.dbpool import pool
from stock.StockUtil import source2
from stock import StockUtil
from Queue import Queue


socket.setdefaulttimeout(5.0)

logging.basicConfig(level=logging.INFO,
                filemode='w',
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/StockDriver.log')

lock = threading.Lock()

def updateStockInfo():
    """
    从网上获取最新股票列表 更新数据库
    使用前  需truncate table
    :param source: 数据源 List [url,获取code的reg]
    :return:
    """
    util = StockUtil.StockUtil()
    webStocks = util.getAllStockInfoFromWeb(source2)

    con = pool.connection()
    cursor = con.cursor()

    for stockInfo in webStocks:
        code = stockInfo.code
        name = stockInfo.name
        try:
            cursor.execute(stockInfo.createInsertSql())
            con.commit()
            logging.info((code,name,'save success'))
        except Exception as e:
            logging.error(e)
            logging.error((stockInfo.code,stockInfo.name,'save failed'))
    con.close()

def updateStocksListeddate(stock):
    """
    跟新股票 上市时间
    :param stock:
    :return:
    """
    util = StockUtil.StockUtil()
    con = pool.connection()
    cursor = con.cursor()

    if stock['listeddate'] != None:
        listeddate = datetime.strftime(stock['listeddate'],'%Y-%m-%d')
    else:
        listeddate=None

    newDate = util.getStockListeddate(stock['code'])
    if newDate != None:
        if newDate == listeddate:
            print stock['code']+"上市时间"+newDate+"无异常"
        else:
            sql = "update stockinfo set listeddate=to_date('"+newDate+"','yyyy-mm-dd') where code='"+stock['code']+"'"
            try:
                cursor.execute(sql)
                con.commit()
            except Exception as e:
                logging.error(e)
                logging.error((stock['code'],stock['name'],'save failed'))
    else:
        print stock['code']+"未找到上市时间"
    con.close()


class updateStockListeddate(threading.Thread):
    def run(self):
        global queue
        util = StockUtil.StockUtil()
        while True:
            if lock.acquire():
                stock = queue.get()
                lock.release()
                print queue.qsize()
                updateStocksListeddate(stock)
            else:
                time.sleep(2)


if __name__=='__main__':

    updateStockInfo()
    # queue = Queue()
    # util = StockUtil.StockUtil()
    # allStock = util.getAllStockInfoFromDB('List')
    # for stock in allStock:
    #     queue.put(stock)
    # print queue.qsize()
    #
    # threads = []
    # for i in range(1):
    #     thread = updateStockListeddate()
    #     threads.append(thread)
    #     thread.start()
    # for a in threads:
    #     a.join()






