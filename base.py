# coding:utf-8
from abc import abstractmethod, ABCMeta
import threading
import redis
from crawler.dbpool import poolRedis
import pickle

class BaseThread(threading.Thread):
    __metaclass__ = ABCMeta
    def __init__(self, inQueue, outQueue, dbQueue ,emptyWait, requestWait, doneMap):
        '''
        线程基类
        :param inQueue: 数据输入队列
        :param outQueue: 数据输出队列
        :param emptyWait: 输入队列为空时等待时间 单位s
        :param requestWait: 网页请求 间隔时间 单位s
        :return:
        '''
        threading.Thread.__init__(self)
        self.inQueue = inQueue
        self.outQueue = outQueue
        self.dbQueue = dbQueue
        self.emptyWait = emptyWait
        self.requestWait = requestWait
        self.doneMap = doneMap

    def whetherDone(self,key):
        return key in self.doneMap

class BaseQueue():
    """
    任务队列 基类  定义了4个 抽象方法
    便于后续更换 队列实现
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def put(self,item):pass

    @abstractmethod
    def get(self):pass

    @abstractmethod
    def empty(self):pass

    @abstractmethod
    def size(self):pass

class TaskQueue(BaseQueue):
    """
    任务队列 线程共享
    封装 Queue实现
    """
    def __init__(self, queue):
        super(TaskQueue, self).__init__()
        self.queue = queue

    def put(self,item):
        self.queue.put(item)

    def get(self):
        return self.queue.get(block=False)

    def empty(self):
        return self.queue.empty()

    def size(self):
        return self.queue._qsize()

class TaskRedisQueue(BaseQueue):
    """
    任务队列 多进程 多线程 共享
    底层使用Redis List实现
    """
    def __init__(self, queueName):
        super(TaskRedisQueue, self).__init__()
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

