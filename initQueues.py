# coding:utf-8
from step import *

if __name__=='__main__':
    task = queues.Job51TaskQueueList()
    task.clearOldData()
    queues = task.queues
    doneMaps = task.doneMaps
    createDownJobTaskQueue(queues['12'])
    AnalysisJobListPage.fillInQueue(queues['23'])
    AnalysisJobInfoPage.fillInQueue(queues['45'])
