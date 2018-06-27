# coding:utf-8
from Queue import Queue
from crawler.base import TaskQueue

class StockTaskQueueList:
    '''
    job1 获取最新全量股票信息
    1、从东方财富页面获取最新所有股票 code,name
    2、逐个股票获取上市时间(用于后面生成季度股价url) 后续可以扩展为存储上市公司信息
    job2 下载缺失的股价信息 数据库单只股票最大时间至当前日期的数据
    3、生成需要下载的URL(新浪股票数据一个季度一个页面)
    4、下载页面
    5、解析页面
    6、数据入库
    '''
    def __init__(self):
        # 各个步骤输入 队列
        self.queues = {}

        tmp='123456'
        for i in range(0,5,1):
            self.queues[tmp[i:i+2]]=TaskQueue(Queue())
            # self.queues[tmp[i:i+2]]=Job51TaskRedisQueue('51jobQueue'+tmp[i:i+2])

        # 各个步骤 已完成 队列
        self.doneMaps = {}
        for m in range(0,5,1):
            self.doneMaps[tmp[m:m+2]]={}

    def clearOldData(self):
        for list in self.queues.values():
            list.queue.queue.clear()

    def toString(self):
        rlt = ''
        tmp='123456'
        tmp1 = ['上市时间获取','股价URL待生成','季度股价待下载','季度股价待解析','数据待入库']

        for i in range(1,5,1):
            rlt += tmp1[i]+':' + str(self.queues[tmp[i:i+2]].size())+'        '
        return rlt


