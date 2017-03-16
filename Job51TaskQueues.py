# coding:utf-8
from Queue import Queue

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
class Job51TaskQueues:
    def __init__(self):
        self.queues = {}

        tmp='123456'
        for i in range(0,5,1):
            self.queues[tmp[i:i+2]]=Queue()

        self.doneMaps = {}
        for m in range(0,5,1):
            self.doneMaps[tmp[m:m+2]]={}

    def toString(self):
        # a = [key+':'+str(self.queues[key]._qsize()) for key in self.queues.iterkeys()]
        # return '\t'.join(a)
        rlt = ''
        tmp='123456'
        for i in range(0,5,1):
            rlt += tmp[i:i+2]+':' + str(self.queues[tmp[i:i+2]]._qsize())+'    '
        return rlt


