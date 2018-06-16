# coding:utf-8
from queues import Job51TaskQueueList
from step import *
from monitor import *
from entity import *
import logging
import random

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [%(threadName)s] %(filename)s %(module)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/51job.log',
                filemode='w')

#文件保存路径
jobListPath = 'D:/fileloc/first/jobList'
jobInfoPath = 'D:/fileloc/first/jobInfo'

def createJobListPageToDB():
    """
    生成所有joblist页面url
    :return:
    """
    con = poolOracle.connection()
    cursor = con.cursor()
    for i in range(1,queues.jobListPageSize,1):
        url = 'https://search.51job.com/list/000000,000000,0000,00,3,99,java,2,'+str(i)+'.html?' \
              'lang=c&stype=1&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99' \
              '&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=5' \
              '&dibiaoid=0&address=&line=&specialarea=00&from=&welfare='
        id = '20180616_java_%s' % (str(i).zfill(5))
        localpath ='%s/%s.html' % (driver.jobListPath,id)
        sql = JobListPage(str(i),id,url,localpath).createInsertSql()
        try:
            cursor.execute(sql)
        except Exception as e:
            logging.error(e)
    con.commit()
    con.close()

if __name__=='__main__':
    createJobListPageToDB()
    
    allThreads={'step2':[],'step3':[],'step4':[],'step5':[],'step6':[],'monitor':[]}

    task = queues.Job51TaskQueueList()
    task.clearOldData()
    queues = task.queues
    doneMaps = task.doneMaps

    monitor = TaskRuntimeMonitor(task, allThreads)
    monitor.start()
    allThreads['monitor'].append(monitor)

    DownJobListPage.fillInQueue(queues['12'])
    AnalysisJobListPage.fillInQueue(queues['23'])
    DownJobInfoPage.fillInQueue(queues['34'])
    AnalysisJobInfoPage.fillInQueue(queues['45'])

    # DownJobListPage.fillDoneQueue(doneMaps['12'])
    # DownJobInfoPage.fillDoneQueue(doneMaps['34'])
    # AnalysisJobInfoPage.fillDoneQueue(doneMaps['45'])

    for i in range(10):
        step2 = DownJobListPage(queues['12'],queues['23'],queues['56'], 0, 5,doneMaps['12'])
        step2.start()
        allThreads['step2'].append(step2)

    for i in range(5):
        step3 = AnalysisJobListPage(queues['23'],queues['34'],queues['56'], 2, 0, doneMaps['23'])
        step3.start()
        allThreads['step3'].append(step3)

    for i in range(20):
        step4 = DownJobInfoPage(queues['34'],queues['45'],queues['56'], 2, random.randint(1,4), doneMaps['34'])
        step4.start()
        allThreads['step4'].append(step4)

    for m in range(10):
        step5 = AnalysisJobInfoPage(queues['45'],queues['56'],queues['56'], 5,0,doneMaps['45'])
        step5.start()
        allThreads['step5'].append(step5)

    for n in range(1):
        step6 = DBExecuter(queues['56'],None, None, 5,0,doneMaps['56'])
        step6.start()
        allThreads['step6'].append(step6)
    for a in allThreads.values():
        for b in a:
            b.join()
