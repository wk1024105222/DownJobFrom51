# coding:utf-8
import logging
import Job51Util
import MyThread

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/DownJobPage.log',
                filemode='a')

class AnalysisJobInfoPage(MyThread):
    def run(self):
        while not self.inQueue.empty():
            jobInfoPageFile = self.inQueue.get()

            jobBean = Job51Util.getJobInfoFromHtml(jobInfoPageFile)['jobbean']
            self.outQueue.put(jobBean)

            logging.info(jobBean.code+ 'Analysis finished filepath:'+jobInfoPageFile)

