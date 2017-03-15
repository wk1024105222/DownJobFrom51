# coding:utf-8
import re
import logging
import Job51Util
import MyThread
import time

class AnalysisJobListPage(MyThread):
    def run(self):
        while True:
            if self.inQueue.empty():
                logging.info('AnalysisJobListPage inQueue is empty wait for '+self.emptyWait+'s')
                time.sleep(self.emptyWait)
                continue
            jobListPageFile = self.inQueue.get()

            try:
                jobListPageHtml = open(jobListPageFile).read()

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
            time.sleep(self.requestWait)