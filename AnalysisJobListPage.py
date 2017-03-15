# coding:utf-8
import os
import re
import logging
import Job51Util
import MyThread
import time
import Job51Driver

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/AnalysisJobListPage.log',
                filemode='a')

class AnalysisJobListPage(MyThread.MyThread):
    def run(self):
        while True:
            time.sleep(self.requestWait)
            if self.inQueue.empty():
                logging.info('AnalysisJobListPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue
            jobListPageFile = self.inQueue.get()

            try:
                jobListPageHtml = open(jobListPageFile, 'rb').read()

                jobs = re.findall(Job51Util.jobUrlReg,jobListPageHtml)
                count=0
                for jobUrl in jobs:
                    self.outQueue.put(jobUrl)
                    count+=1
                logging.info(jobListPageFile+ '    Analysis finished getJobInfo:'+str(count))
            except Exception as e:
                logging.error(e)
                logging.error(jobListPageFile+'    Analysis failed')
                continue

    def fillInQueue(self):
        for filename in os.listdir(Job51Driver.jobListPath):
            self.inQueue.put(Job51Driver.jobListPath+'/'+filename)