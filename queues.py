# coding:utf-8
from Queue import Queue
from base import BaseQueue

#class Job51TaskQueues():
#招聘信息查询结果 页数
jobListPageSize=2000

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
    def __init__(self):
        self.queue = Queue()

    def put(self,item):
        self.queue.put(item)

    def get(self):
        return self.queue.get()

    def empty(self):
        return self.queue.empty()

    def size(self):
        return self.queue._qsize()


class Job51TaskQueueList:
    def __init__(self):
        # 各个步骤输入 队列
        self.queues = {}

        tmp='123456'
        for i in range(0,5,1):
            self.queues[tmp[i:i+2]]=Job51TaskQueue()

        # 各个步骤 已完成 队列
        self.doneMaps = {}
        for m in range(0,5,1):
            self.doneMaps[tmp[m:m+2]]={}

    def toString(self):
        rlt = ''
        tmp='123456'
        for i in range(0,5,1):
            rlt += tmp[i:i+2]+':' + str(self.queues[tmp[i:i+2]].size())+'    '
        return rlt


