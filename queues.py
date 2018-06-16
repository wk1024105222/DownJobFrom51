# coding:utf-8
from Queue import Queue
import redis
from base import BaseQueue
from dbpool import poolRedis
import pickle
import logging

#class Job51TaskQueues():
#招聘信息查询结果 页数
jobListPageSize=1960

#d第一步 使用查询结果页数填充 队列
# jobListPageUrlQueue = Queue()
#第二部 下载每页查询结果
# jobListPageQueue = Queue()
#第三部 解析每页查询结果 获取职位 URL
# jobInfoPageUrlQueue = Queue()
#第四步 下载职位信息页面
# jobInfoPageQueue = Queue()
#第五步 解析职位页面
# jobInfoBeanQueue = Queue()
#第六部 入库
class Job51TaskQueue(BaseQueue):
    """
    51job 招聘信息 下载 任务队列 线程共享
    封装 Queue实现
    """
    def __init__(self, queue):
        super(Job51TaskQueue, self).__init__()
        self.queue = queue

    def put(self,item):
        self.queue.put(item)

    def get(self):
        return self.queue.get(block=False)

    def empty(self):
        return self.queue.empty()

    def size(self):
        return self.queue._qsize()

class Job51TaskRedisQueue(BaseQueue):
    """
    51job 招聘信息 下载 任务队列 多进程 多线程 共享
    底层使用Redis List实现
    """
    def __init__(self, queueName):
        super(Job51TaskRedisQueue, self).__init__()
        self.queueName = queueName
        self.connection = redis.Redis(connection_pool=poolRedis)

    def put(self,item):
        self.connection.rpush(self.queueName, pickle.dumps(item))

    def get(self):
        # logging.info('self.connection.llen(self.queueName):    '+ str(self.connection.llen(self.queueName)))
        tmp = self.connection.lpop(self.queueName)
        if tmp == None:
            return None
        else:
            return  pickle.loads(tmp)

    def empty(self):
        if not self.connection.exists(self.queueName) :
            return True
        else:
            return  self.connection.llen(self.queueName) == 0

    def size(self):
        return self.connection.llen(self.queueName)

    def clear(self):
        if self.connection.exists(self.queueName):
            self.connection.delete(self.queueName)



class Job51TaskQueueList:
    def __init__(self):
        # 各个步骤输入 队列
        self.queues = {}

        tmp='123456'
        for i in range(0,5,1):
            self.queues[tmp[i:i+2]]=Job51TaskQueue(Queue())
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
        tmp1 = ['列表待下载','列表待解析','明细待下载','明细待解析','数据待入库']

        for i in range(0,5,1):
            rlt += tmp1[i]+':' + str(self.queues[tmp[i:i+2]].size())+'        '
        return rlt


