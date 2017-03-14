# coding:utf-8
from Queue import Queue
import os
import re
import threading
import urllib
import urllib2
import cookielib
import logging
import datetime
import time
import Job51Util
import Job51TaskQueues

class AnalysisJobListPage(threading.Thread):
    def run(self):
        while not Job51TaskQueues.jobListPageQueue.empty():
            jobListPageFile = Job51TaskQueues.jobListPageQueue.get()

            try:
                jobListPageHtml = open(jobListPageFile).read()

                jobs = re.findall(Job51Util.jobUrlReg,jobListPageHtml)
                count=0
                for jobUrl in jobs:
                    Job51TaskQueues.jobInfoPageUrlQueue.put(jobUrl)
                    count+=1

                logging.info(jobListPageFile+ '    Analysis finished getJobInfo:'+str(count))

            except Exception as e:
                logging.error(e)
                logging.error(jobListPageFile+'    Analysis failed')
                continue
