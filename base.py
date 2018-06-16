# coding:utf-8
from abc import abstractmethod, ABCMeta
import threading

class BaseThread(threading.Thread):
    """
    从51job下载 职位包含java 的job 每个job以html保存本地
    """
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

    # # 填充 数据输入队列
    # @abstractmethod
    # @staticmethod
    # def fillInQueue(inQueue):pass
    #
    # # 填充已完成的队列 用于过滤 输入队列已完成的数据
    # @staticmethod
    # @abstractmethod
    # def fillDoneQueue(doneQueue):pass

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

