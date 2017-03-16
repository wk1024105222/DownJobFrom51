# coding:utf-8
from queues import Job51TaskQueues
from step import *
from monitor import *

#文件保存路径
jobListPath = 'D:/fileloc/2017-03-15_51job_java/jobList'
jobInfoPath = 'D:/fileloc/2017-03-15_51job_java/jobInfo'
# jobInfoPath = 'v:/job'

if __name__=='__main__':
    task = queues.Job51TaskQueues()
    queues = task.queues
    doneMaps = task.doneMaps
    createDownJobTaskQueue(queues['12'])

    allThreads = []
    DownJobListPage.fillDoneQueue(doneMaps['12'])
    for i in range(20):
        step2 = DownJobListPage(queues['12'],queues['23'], 0, 5,doneMaps['12'])
        step2.start()
        allThreads.append(step2)

    AnalysisJobListPage.fillInQueue(queues['23'])
    for i in range(10):
        step3 = AnalysisJobListPage(queues['23'],queues['34'], 2, 0, doneMaps['23'])
        step3.start()
        allThreads.append(step3)

    DownJobInfoPage.fillDoneQueue(doneMaps['34'])
    for i in range(20):
        step4 = DownJobInfoPage(queues['34'],queues['45'], 2, 3, doneMaps['34'])
        step4.start()
        allThreads.append(step4)

    AnalysisJobInfoPage.fillInQueue(queues['45'])
    AnalysisJobInfoPage.fillDoneQueue(doneMaps['45'])
    for m in range(1):
        step5 = AnalysisJobInfoPage(queues['45'],queues['56'], 5,0,doneMaps['45'])
        step5.start()
        allThreads.append(step5)


    # SaveJobInfoToDB.SaveJobInfoToDB.fillDoneQueue(doneMaps['56'])
    for n in range(1):
        step6 = SaveJobInfoToDB(queues['56'], None, 5,0,doneMaps['56'])
        step6.start()
        allThreads.append(step6)

    monitor = TaskRuntimeMonitor(task, allThreads)
    monitor.start()
    allThreads.append(monitor)

    for a in allThreads:
        a.join()
