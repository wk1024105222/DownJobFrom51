# coding:utf-8
from Queue import Queue
import re
import logging
import Job51Util
import MyThread

class AnalysisJobListPage(MyThread):
    def run(self):
        while not self.inQueue.empty():
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
