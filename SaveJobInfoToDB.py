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
                logging.info('save task is empty sleep 1 min')
                time.sleep(60)

            jobBean = self.inQueue.get()
            sqls.append( jobBean.createInsertSql())
            count+=1
            if count==100:
                Job51Util.executDMLSql(sqls)
                sqls=[]
                count=0