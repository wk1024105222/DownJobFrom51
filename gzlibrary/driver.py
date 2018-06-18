# coding:utf-8
from queues import GzlibTaskQueueList
from step import *
from monitor import *
from entity import *
import logging
import random

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s\t%(thread)d\t[%(threadName)s]\t%(filename)s\t%(module)s\t%(funcName)s\t[line:%(lineno)d]\t%(levelname)s\t%(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/gzlib.log',
                filemode='w')

if __name__=='__main__':

    allThreads={'step1':[],'step2':[],'step3':[],'step4':[],'monitor':[]}

    task = GzlibTaskQueueList()
    task.clearOldData()
    queues = task.queues
    doneMaps = task.doneMaps

    monitor = TaskRuntimeMonitor(task, allThreads)
    monitor.start()
    allThreads['monitor'].append(monitor)

    DownBookHoldJson.fillInQueue(queues['12'])
    AnalysisBookHoldJson.fillInQueue(queues['23'])

    for i in range(10):
        step2 = DownBookHoldJson(queues['12'],queues['23'],queues['34'], 0, 2,doneMaps['12'])
        step2.start()
        allThreads['step2'].append(step2)

    # for i in range(1):
    #     step3 = AnalysisBookHoldJson(queues['23'],queues['34'],queues['34'], 2, 0, doneMaps['23'])
    #     step3.start()
    #     allThreads['step3'].append(step3)

    for n in range(1):
        step6 = DBExecuter(queues['34'],None, None, 5,0,doneMaps['34'])
        step6.start()
        allThreads['step4'].append(step6)

    for a in allThreads.values():
       for b in a:
          b.join()
