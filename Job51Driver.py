# coding:utf-8
import Job51TaskQueues
import DownJobListPage
import AnalysisJobListPage
import DownJobInfoPage
import AnalysisJobInfoPage
import SaveJobInfoToDB

#文件保存路径
jobListPath = 'd:/fileloc/jobList'
jobInfoPath = 'd:/fileloc/jobInfo'

if __name__=='__main__':
    a = Job51TaskQueues.Job51TaskQueues()
    queues = a.queues
    DownJobListPage.createDownJobTaskQueue(queues['12']);

    allThreads = []
    # for i in range(1):
    #     step2 = DownJobListPage.DownJobListPage(queues['12'],queues['23'], 0, 5)
    #     step2.start()
    #     allThreads.append(step2)

    # for i in range(1):
    #     step3 = AnalysisJobListPage.AnalysisJobListPage(queues['23'],queues['34'], 5, 0)
    #     step3.fillInQueue()
    #     step3.start()
    #     allThreads.append(step3)
    #
    # for i in range(1):
    #     step4 = DownJobInfoPage.DownJobInfoPage(queues['34'],queues['45'], 5, 5)
    #     step4.start()
    #     allThreads.append(step4)

    for i in range(1):
        step5 = AnalysisJobInfoPage.AnalysisJobInfoPage(queues['45'],queues['56'], 5,0)
        step5.fillInQueue()
        step5.start()
        allThreads.append(step5)

    for i in range(1):
        step6 = SaveJobInfoToDB.SaveJobInfoToDB(queues['56'], None, 5,0)
        step6.start()
        allThreads.append(step5)

    for a in allThreads:
        a.join()
