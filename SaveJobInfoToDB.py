import MyThread
import logging
import time
import Job51Util

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/DownJobPage.log',
                filemode='a')

class SaveJobInfoToDB(MyThread):
    def run(self):
        sqls=[]
        while True:
            if self.inQueue.empty():
                logging.info('SaveJobInfoToDB inQueue is empty wait for '+self.emptyWait+'s')
                time.sleep(self.emptyWait)

            jobBean = self.inQueue.get()
            sqls.append( jobBean.createInsertSql())
            count+=1
            if count==100:
                Job51Util.executDMLSql(sqls)
                sqls=[]
                count=0
            time.sleep(self.requestWait)