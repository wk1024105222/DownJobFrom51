# coding:utf-8
import MyThread
import logging
import time
import Job51Util

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/SaveJobInfoToDB.log',
                filemode='a')

class SaveJobInfoToDB(MyThread.MyThread):
    def run(self):
        sqls=[]
        count=0
        while True:
            time.sleep(self.requestWait)
            if self.inQueue.empty():
                logging.info('SaveJobInfoToDB inQueue is empty wait for '+str(self.emptyWait)+'s')
                Job51Util.executDMLSql(sqls)
                sqls=[]
                time.sleep(self.emptyWait)
                continue

            jobBean = self.inQueue.get()
            sql = jobBean.createInsertSql()
            # print sql
            sqls.append(sql)
            count+=1
            if count==100:
                Job51Util.executDMLSql(sqls)
                sqls=[]
                count=0

    def fillInQueue(self):
        print "None"
