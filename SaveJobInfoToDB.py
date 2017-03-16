# coding:utf-8
import MyThread
import logging
import time
import Job51Util

class SaveJobInfoToDB(MyThread.MyThread):
    def run(self):
        sqls=[]
        count=0
        emptyNum=0
        while True:
            if self.inQueue.empty():
                if emptyNum>50:
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
            if super(SaveJobInfoToDB, self).whetherDone(jobBean.code):
                logging.info('[jobInfoCode: '+jobBean.code+'] had been saved')
                continue
            sql = jobBean.createInsertSql()
            sqls.append(sql)
            count+=1
            if count==100:
                Job51Util.executDMLSql(sqls)
                logging.info('SaveJobInfoToDB insert into 100 records ')
                sqls=[]
                count=0
            time.sleep(self.requestWait)
        return

    def fillInQueue(inQueue):
        return

    @staticmethod
    def fillDoneQueue(doneQueue):
        doneQueue.clear()
        codes = Job51Util.getListFromDB("select distinct code from job51")
        doneQueue.update({code[0]: 1 for code in codes})
        logging.info('loadBeenSavedJobInfo: '+str(len(doneQueue))+' successed')

