# coding:utf-8

import threading
import logging
import Job51Util
import Job51TaskQueues

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/DownJobPage.log',
                filemode='a')

class AnalysisJobInfoPage(threading.Thread):
    def run(self):
        while not Job51TaskQueues.jobInfoPageQueue.empty():
            jobInfoPageFile = Job51TaskQueues.jobInfoPageQueue.get()

            jobBean = Job51Util.getJobInfoFromHtml(jobInfoPageFile)['jobbean']
            Job51TaskQueues.jobInfoBeanQueue.put(jobBean)

            logging.info(jobBean.code+ 'Analysis finished filepath:'+jobInfoPageFile)

