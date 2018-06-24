# coding:utf-8
from datetime import date, datetime
import logging
import os
import re
import urllib
import uuid

from stock.entity import StockPrice, StockInfo
from stock.dbpool import pool

#同花顺 不全
source1=['http://bbs.10jqka.com.cn/codelist.html',
         re.compile(r' <li><a href="http://bbs.10jqka.com.cn/[a-z]{2},\d{6}.*?>'
                    r'(.*?[6,0,3]\d{5})</a></li>')]


#东方财富网 最全
source2=['http://quote.eastmoney.com/stocklist.html',
         re.compile(r'<li><a target="_blank" href="http://quote.eastmoney.com/[a-z]{2}\d{6}.html">'
                    r'(.*?)\(([6,0,3]\d{5})\)</a></li>')]

logging.basicConfig(level=logging.INFO,
                filemode='w',
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/StockUtil.log')


def getSinaSeasonUrl(code, beginDate):
    """
    获取（begindate,today]时间区间的 urls
    :param code: 股票代码
    :param beginDate: 数据库数据最大交易日期 下载的开始日期
    :return: urls 一个季度的日交易数据 一个页面
    """

    begin_year = beginDate.year
    begin_month = beginDate.month
    begin_season = getSeasonByMonth(begin_month)

    end_year = date.today().year
    end_month = date.today().month
    end_season = getSeasonByMonth(end_month)
    urls = []
    i = 0
    while (begin_year * 10 + begin_season) <= (end_year * 10 + end_season):
        urls.append('http://money.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/%s.phtml?year=%d&jidu=%d' % (code, begin_year, begin_season))
        begin_season += 1
        if begin_season == 5:
            begin_year += 1
            begin_season = 1
    return urls

def getSeasonByMonth(month):
    if month >= 1 and month <= 3:
        season = 1
    elif month >= 4 and month <= 6:
        season = 2
    elif month >= 7 and month <= 9:
        season = 3
    elif month >= 10 and month <= 12:
        season = 4
    return season

def downLackPriceFromSinaBySeason(code, maxDate):
    """
    下载缺失数据 的html文件 保存到本地
    :param code: 股票代码
    :param maxDate: 数据库数据最大交易日期 下载的开始日期
    :return:
    """

    urls = getSinaSeasonUrl(code, maxDate)

    path = 'd:/fileloc/stock/'+code+'/'
    if not os.path.exists(path):
        os.mkdir(path)

    for url in urls:
        filename  = path+str(code)+'_'+url[-11:-7]+'_'+url[-1]+'.html'
        if not os.path.exists(filename):
            try:
                urllib.urlretrieve(url,filename+'.tmp')
                os.renames(filename+'.tmp',filename)

            except Exception as e:
                print e

def getStockListeddate(code):
    """
    根据代码 获取上市时间
    :param code:
    :return:上市时间/None
    """

    url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpInfo/stockid/"+code+".phtml"

    html = urllib.urlopen(url).read().decode('gb2312','ignore').encode('utf8')

    dateReg = re.compile('<td class="ct">上市日期[\s\S]*?(\d{4}-\d{2}-\d{2})</a></td>')

    date = re.findall(dateReg, html)

    if len(date)==1:
        return date[0]
    return None

def getPriceFromHtml(self, code,html):
    """

    :param html: 一个季度页面的html
    :return: [] 日交易数据
    """
    stockPrices = []
    reg = re.compile(r'<td><div align="center">\s+'
                    r'.*?\s*(\d{4}-\d{2}-\d{2})\s+.*?\s*</div></td>\s*'
                    r'<td[^\d]*([^<]*)</div></td>\s+'
                    r'<td[^\d]*([^<]*)</div></td>\s+'
                    r'<td[^\d]*([^<]*)</div></td>\s+'
                    r'<td[^\d]*([^<]*)</div></td>\s+'
                    r'<td[^\d]*([^<]*)</div></td>\s+'
                    r'<td[^\d]*([^<]*)</div></td>\s+'
                    )

    rlt = re.findall(reg, html)

    try:
        for x in rlt:
            tmpdate = datetime.strptime(x[0], "%Y-%m-%d")
            # if tmpdate >= lowDate:
            stockPrices.append(StockPrice(('%s' % uuid.uuid1())[:32] ,code, tmpdate, x[1], x[2], x[4], x[3], x[5]))
    except Exception as e:
        logging.info(str(e))
        stockPrices = []
    return stockPrices

def getAllStockInfoFromDB(self,type):
    """
    获取数据库股票信息到缓存
    :param type: List Dict 决定了返回类型
    :return:
    """
    #con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai")
    con = pool.connection()
    cursor = con.cursor()
    cursor.execute("select code, listeddate, name from STOCKINFO where listeddate is null")
    stockInfos = cursor.fetchall()
    con.close()

    if type == 'Dict':
        allStock = {}
        for x in stockInfos:
            tmp=[x[1],x[2]]
            allStock[x[0]]=tmp
    elif type == 'List':
        allStock = []
        for y in stockInfos:
            allStock.append({'code':y[0],'listeddate':y[1],'name':y[2]})
    else:
        None
    return allStock

def getAllStockInfoFromWeb(self, source):
    """
    从网上获取最新股票列表
    :param source: 数据源 List [url,获取code的reg]
    :return: 股票列表
    """

    rlt=[]

    url = source[0]
    pageHtml=urllib.urlopen(url).read()

    stockCodeReg = source[1]
    stockCodes = re.findall(stockCodeReg, pageHtml)

    for tmp in stockCodes:
        if(len(tmp) == 2):
            code = tmp[1]
            #name = tmp[0].decode('gb2312','ignore').encode('utf8')
            name = tmp[0]
            rlt.append(StockInfo(code=code, name=name))

    return rlt

