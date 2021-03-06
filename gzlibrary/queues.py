# coding:utf-8
from Queue import Queue
from crawler.base import TaskQueue

#d第一步 使用查询结果页数填充 队列
#
#第二部 下载每页查询结果
# DownBookHoldJson
#第三部 解析每页查询结果 获取职位 URL
# AnalysisBookHoldJson
#第4部 入库
# DBExecuter
class GzlibTaskQueueList:
    def __init__(self):
        # 各个步骤输入 队列
        self.queues = {}

        tmp='1234'
        for i in range(0,3,1):
            self.queues[tmp[i:i+2]]=TaskQueue(Queue())
            # self.queues[tmp[i:i+2]]=Job51TaskRedisQueue('51jobQueue'+tmp[i:i+2])

        # 各个步骤 已完成 队列
        self.doneMaps = {}
        for m in range(0,3,1):
            self.doneMaps[tmp[m:m+2]]={}

    def clearOldData(self):
        for list in self.queues.values():
            list.queue.queue.clear()

    def toString(self):
        rlt = ''
        tmp='123456'
        tmp1 = ['列表待下载','列表待解析','数据待入库']

        for i in range(0,3,1):
            rlt += tmp1[i]+':' + str(self.queues[tmp[i:i+2]].size())+'        '
        return rlt


