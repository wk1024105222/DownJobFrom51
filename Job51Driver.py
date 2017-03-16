# coding:utf-8
import Job51TaskQueues
import DownJobListPage
import AnalysisJobListPage
import DownJobInfoPage
import AnalysisJobInfoPage
import SaveJobInfoToDB
import time
import TaskRuntimeMonitor

#文件保存路径
jobListPath = 'd:/fileloc/jobList'
jobInfoPath = 'd:/fileloc/jobInfo'

if __name__=='__main__':
    task = Job51TaskQueues.Job51TaskQueues()
    queues = task.queues
    doneMaps = task.doneMaps
    DownJobListPage.createDownJobTaskQueue(queues['12'])

    allThreads = []
    DownJobListPage.DownJobListPage.fillDoneQueue(doneMaps['12'])
    for i in range(20):
        step2 = DownJobListPage.DownJobListPage(queues['12'],queues['23'], 0, 5,doneMaps['12'])
        step2.start()
        allThreads.append(step2)

    AnalysisJobListPage.AnalysisJobListPage.fillInQueue(queues['23'])
    for i in range(10):
        step3 = AnalysisJobListPage.AnalysisJobListPage(queues['23'],queues['34'], 1, 0, doneMaps['23'])
        step3.start()
        allThreads.append(step3)

    DownJobInfoPage.DownJobInfoPage.fillDoneQueue(doneMaps['34'])
    for i in range(30):
        step4 = DownJobInfoPage.DownJobInfoPage(queues['34'],queues['45'], 1, 1, doneMaps['34'])
        step4.start()
        allThreads.append(step4)

    AnalysisJobInfoPage.AnalysisJobInfoPage.fillInQueue(queues['45'])
    AnalysisJobInfoPage.AnalysisJobInfoPage.fillDoneQueue(doneMaps['45'])
    for i in range(100):
        step5 = AnalysisJobInfoPage.AnalysisJobInfoPage(queues['45'],queues['56'], 2,0,doneMaps['45'])
        step5.start()
        allThreads.append(step5)

    SaveJobInfoToDB.SaveJobInfoToDB.fillDoneQueue(doneMaps['56'])
    for i in range(10):
        step6 = SaveJobInfoToDB.SaveJobInfoToDB(queues['56'], None, 5,0,doneMaps['56'])
        step6.start()
        allThreads.append(step6)

    monitor = TaskRuntimeMonitor.TaskRuntimeMonitor(task)
    monitor.start()
    allThreads.append(monitor)

    for a in allThreads:
        a.join()
