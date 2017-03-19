# coding:utf-8
from queues import Job51TaskQueueList
from step import *
from monitor import *
import logging

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [%(threadName)s] %(filename)s %(module)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/51job.log',
                filemode='a')

#文件保存路径
jobListPath = 'D:/fileloc/first/jobList'
jobInfoPath = 'D:/fileloc/first/jobInfo'
# jobInfoPath = 'v:/job'

if __name__=='__main__':
    task = queues.Job51TaskQueueList()
    queues = task.queues
    doneMaps = task.doneMaps

    allThreads={'step2':[],'step3':[],'step4':[],'step5':[],'step6':[],'monitor':[]}

    DownJobListPage.fillDoneQueue(doneMaps['12'])
    DownJobInfoPage.fillDoneQueue(doneMaps['34'])
    AnalysisJobInfoPage.fillDoneQueue(doneMaps['45'])

    for i in range(20):
        step2 = DownJobListPage(queues['12'],queues['23'], 0, 5,doneMaps['12'])
        step2.start()
        allThreads['step2'].append(step2)

    for i in range(10):
        step3 = AnalysisJobListPage(queues['23'],queues['34'], 2, 0, doneMaps['23'])
        step3.start()
        allThreads['step3'].append(step3)

    for i in range(20):
        step4 = DownJobInfoPage(queues['34'],queues['45'], 2, 3, doneMaps['34'])
        step4.start()
        allThreads['step4'].append(step4)

    for m in range(5):
        step5 = AnalysisJobInfoPage(queues['45'],queues['56'], 5,0,doneMaps['45'])
        step5.start()
        allThreads['step5'].append(step5)

    for n in range(5):
        step6 = SaveJobInfoToDB(queues['56'], None, 5,0,doneMaps['56'])
        step6.start()
        allThreads['step6'].append(step6)

    monitor = TaskRuntimeMonitor(task, allThreads)
    monitor.start()
    allThreads['monitor'].append(monitor)

    for a in allThreads.values():
        for b in a:
            b.join()
