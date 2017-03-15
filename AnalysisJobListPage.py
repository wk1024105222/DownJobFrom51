# coding:utf-8
import os
import re
import logging
import Job51Util
import MyThread
import time
import Job51Driver

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [%(threadName)s] %(filename)s %(module)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/AnalysisJobListPage.log',
                filemode='a')

class AnalysisJobListPage(MyThread.MyThread):
    def run(self):
        emptyNum=0
        while True:
            time.sleep(self.requestWait)
            if self.inQueue.empty():
                if emptyNum>10:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                emptyNum+=1
                # logging.info('AnalysisJobListPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue
            # 计数归零
            emptyNum=0
            jobListPageFile = self.inQueue.get()

            try:
                jobListPageHtml = open(jobListPageFile, 'rb').read()

                jobs = re.findall(Job51Util.jobUrlReg,jobListPageHtml)
                count=0
                for jobUrl in jobs:
                    self.outQueue.put(jobUrl)
                    count+=1
                logging.info('['+jobListPageFile+ '] Analysis successed getJobInfoUrl:'+str(count))
            except Exception as e:
                logging.error(e)
                logging.error('['+jobListPageFile+'] Analysis failed')
        return

    def fillInQueue(self):
        for filename in os.listdir(Job51Driver.jobListPath):
            self.inQueue.put(Job51Driver.jobListPath+'/'+filename)