# coding:utf-8
import logging
import os
import Job51Util
import MyThread
import time
import Job51Driver

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/AnalysisJobInfoPage.log',
                filemode='a')

class AnalysisJobInfoPage(MyThread.MyThread):
    def run(self):
        while True:
            time.sleep(self.requestWait)
            if self.inQueue.empty():
                logging.info('AnalysisJobInfoPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue
            jobInfoPageFile = self.inQueue.get()

            jobBean = Job51Util.getJobInfoFromHtml(jobInfoPageFile)['jobbean']
            self.outQueue.put(jobBean)

            logging.info(jobBean.code+ 'Analysis finished filepath:'+jobInfoPageFile)


    def fillInQueue(self):
        for filename in os.listdir(Job51Driver.jobInfoPath):
            self.inQueue.put(Job51Driver.jobInfoPath+'/'+filename)
