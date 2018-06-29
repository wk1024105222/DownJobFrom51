# coding:utf-8
import time
import urllib2
from datetime import date, datetime
import logging
import os
import re
import urllib
import uuid
from stock.entity import StockPrice, StockInfo

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

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Cookie': 'U_TRS1=00000016.ac56e37.5857990f.d18a72f7; vjuids=-2990d039e.159162e00bf.0.9f0f8b87; SINAGLOBAL=221.5.109.16_1482220820.812250; SCF=AjzsqsRRtfJKCanrAtHDxL3fl1Km9ZivBmNHpXLeC3sBFn3AK96PNWzB2cGMnXf6zNM85_Wzi_jBUWJDQgb3zzc.; SGUID=1489717243345_8bd09b67; UOR=,,; SR_SEL=1_511; FINA_V_S_2=sh600115; lxlrttp=1524018574; __utma=269849203.614057752.1516415338.1524068358.1524148902.3; __utmz=269849203.1524148902.3.3.utmcsr=2345.com|utmccn=(referral)|utmcmd=referral|utmcct=/; ULV=1526128398866:12:1:1::1524148851514; vjlast=1489717243.1526399507.10; SUB=_2AkMscbsBf8NxqwJRmP4Ry2jraI1zzwnEieKaLUraJRMyHRl-yD83qm8gtRB6B_GV7nKmkpA4bNeTuWVAFWwsUPWcLs8k; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WWMe2xPH7hdhK_E6pj2XoRJ; U_TRS2=0000001a.c7351b53.5b365a49.2fe7883b; FINANCE2=9edf0b4b86d8949c74ea437271f2bb17',
                'Host': 'vip.stock.finance.sina.com.cn',
                'If-Modified-Since': '%s' % (time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.localtime())),
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
              }
    data = None
    req = urllib2.Request(url, data, headers)
    response = urllib2.urlopen(req)
    html = response.read().decode('gb2312','ignore').encode('utf8')

    # html = urllib.urlopen(url).read()#.decode('gb2312','ignore').encode('utf8')

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



