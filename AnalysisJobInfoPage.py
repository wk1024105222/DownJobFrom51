# coding:utf-8
import logging
import os
import Job51Util
import MyThread
import time
import Job51Driver
import SaveJobInfoToDB

class AnalysisJobInfoPage(MyThread.MyThread):
    def run(self):
        emptyNum=0
        while True:
            if self.inQueue.empty():
                if emptyNum>50:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                emptyNum+=1
                # logging.info('AnalysisJobInfoPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue
            emptyNum=0
            jobInfoPageFile = self.inQueue.get()
            if super(AnalysisJobInfoPage, self).whetherDone(jobInfoPageFile.split('/')[-1][0:8]):
                logging.info(jobInfoPageFile + ' had been saved so also had been analysis')
                continue

            jobBean = Job51Util.getJobInfoFromHtml(jobInfoPageFile)['jobbean']
            self.outQueue.put(jobBean)

            logging.info('[jobInfo'+jobBean.code+ '] Analysis successed filepath:'+jobInfoPageFile)
            time.sleep(self.requestWait)
        logging.info('thread is over')
        return

    @staticmethod
    def fillInQueue(inQueue):
        for filename in os.listdir(Job51Driver.jobInfoPath):
            # 过滤异常文件
            if not filename.endswith('.err'):
                inQueue.put(Job51Driver.jobInfoPath+'/'+filename)

    @staticmethod
    def fillDoneQueue(doneQueue):
        SaveJobInfoToDB.SaveJobInfoToDB.fillDoneQueue(doneQueue)
