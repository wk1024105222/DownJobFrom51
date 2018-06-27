# coding:utf-8
from crawler.stock.queues import StockTaskQueueList
from crawler.stock.step import UpdateStockInfo, GetLackPriceHtmlUrl, DownLackPriceHtml, AnalysisLackPriceHtml, \
    GetAllListedStock, DBExecuter
from crawler.stock.monitor import TaskRuntimeMonitor
import logging

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s\t%(thread)d\t[%(threadName)s]\t%(filename)s\t%(module)s\t%(funcName)s\t[line:%(lineno)d]\t%(levelname)s\t%(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/stockinfo.log',
                filemode='w')

if __name__=='__main__':
    '''
    job1 获取最新全量股票信息
    1、从东方财富页面获取最新所有股票 code,name
    2、逐个股票获取上市时间(用于后面生成季度股价url) 后续可以扩展为存储上市公司信息

    job2 下载缺失的股价信息 数据库单只股票最大时间至当前日期的数据
    1、生成需要下载的URL(新浪股票数据一个季度一个页面)
    2、下载页面
    3、解析页面 数据入库
    '''

    allThreads = {'step1': [], 'step2': [], 'step3': [], 'step4': [], 'step5': [], 'step6': [], 'monitor': []}

    task = StockTaskQueueList()
    task.clearOldData()
    queues = task.queues
    doneMaps = task.doneMaps

    # monitor = TaskRuntimeMonitor(task, allThreads)
    # monitor.start()
    # allThreads['monitor'].append(monitor)

    # UpdateStockInfo.fillInQueue(queues['12'])
    # GetLackPriceHtmlUrl.fillInQueue(queues['23'])
    # DownLackPriceHtml.fillInQueue(queues['34'])
    # AnalysisLackPriceHtml.fillInQueue(queues['45'])
    #
    GetAllListedStock.fillDoneQueue(doneMaps['12'])
    # GetLackPriceHtmlUrl.fillDoneQueue(queues['23'])
    # DownLackPriceHtml.fillDoneQueue(queues['34'])
    # AnalysisLackPriceHtml.fillDoneQueue(queues['45'])

    for i in range(1):
        step1 = GetAllListedStock(None,queues['12'],queues['56'], 0, 2,doneMaps['12'])
        step1.start()
        allThreads['step1'].append(step1)

    for i in range(20):
        step2 = UpdateStockInfo(queues['12'],queues['23'],queues['56'], 0, 2,None)
        step2.start()
        allThreads['step2'].append(step2)
    #
    # for i in range(10):
    #     step3 = GetLackPriceHtmlUrl(queues['23'],queues['34'],queues['56'], 2, 0, None)
    #     step3.start()
    #     allThreads['step3'].append(step3)
    #
    # for i in range(10):
    #     step4 = DownLackPriceHtml(queues['34'],queues['45'],queues['56'], 2, 0, None)
    #     step4.start()
    #     allThreads['step4'].append(step4)
    #
    # for i in range(10):
    #     step5 = AnalysisLackPriceHtml(queues['45'],None,queues['56'], 2, 0, None)
    #     step5.start()
    #     allThreads['step5'].append(step4)
    #
    for n in range(1):
        step6 = DBExecuter(queues['56'],None, None, 5,0,None)
        step6.start()
        allThreads['step6'].append(step6)

    for a in allThreads.values():
       for b in a:
          b.join()
