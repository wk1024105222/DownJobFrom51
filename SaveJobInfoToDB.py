# coding:utf-8
import MyThread
import logging
import time
import Job51Util

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [%(threadName)s] %(filename)s %(module)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/SaveJobInfoToDB.log',
                filemode='a')

class SaveJobInfoToDB(MyThread.MyThread):
    def run(self):
        sqls=[]
        count=0
        emptyNum=0
        while True:
            time.sleep(self.requestWait)
            if self.inQueue.empty():
                if emptyNum>10:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                emptyNum+=1

                # logging.info('SaveJobInfoToDB inQueue is empty wait for '+str(self.emptyWait)+'s')

                Job51Util.executDMLSql(sqls)
                sqls=[]
                count=0

                time.sleep(self.emptyWait)
                continue

            emptyNum=0
            jobBean = self.inQueue.get()
            sql = jobBean.createInsertSql()
            sqls.append(sql)
            count+=1
            if count==100:
                Job51Util.executDMLSql(sqls)
                logging.info('SaveJobInfoToDB insert into 100 records ')
                sqls=[]
                count=0
        return

    def fillInQueue(self):
        print "None"
