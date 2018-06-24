# coding:utf-8
from datetime import date, datetime
import logging
import os
import re
import urllib
import uuid

from stock.entity import StockPrice, StockInfo
from stock.dbpool import pool

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



