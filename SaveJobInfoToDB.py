# coding:utf-8
import os
import cx_Oracle
import MyThread
import logging
import time
import Job51Util
import Job51Driver

class SaveJobInfoToDB(MyThread.MyThread):
    def run(self):
        con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai")
        cursor = con.cursor()
        emptyNum=0
        while True:
            if self.inQueue.empty():
                if emptyNum>50:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                emptyNum+=1
                logging.info('SaveJobInfoToDB inQueue is empty wait for '+str(self.emptyWait)+'s emptyNum:'+str(emptyNum))
                time.sleep(self.emptyWait)
                continue

            emptyNum=0
            jobBean = self.inQueue.get()
            if super(SaveJobInfoToDB, self).whetherDone(jobBean.code):
                logging.info('[jobInfoCode: '+jobBean.code+'] had been saved')
                continue
            sql = jobBean.createInsertSql()
            try:
                cursor.execute(sql.encode('gb2312','ignore'))
                con.commit()
                logging.info('[jobInfoCode: '+jobBean.code+'] saved successed')
            except Exception as e:
                logging.error(e)
                # logging.error("excute sql %s failed" % (sql))
                filename = Job51Driver.jobInfoPath+'/'+jobBean.code+'.html'
                filename_tmp = Job51Driver.jobInfoPath+'/'+jobBean.code+'.html.tmp'
                try:
                    if os.path.exists(filename):
                        logging.error("find error file %s" % (filename))
                        os.renames(filename,filename+".err")
                    else:
                        if os.path.exists(filename_tmp):
                            logging.error("find error file %s" % (filename_tmp))
                            os.renames(filename_tmp,filename_tmp+".err")
                except Exception as e:
                    logging.error(e)
                    logging.error("error file %s rename failed " % (filename))
                continue

            time.sleep(self.requestWait)
        con.close()
        logging.info('thread is over')
        return

    def fillInQueue(inQueue):
        return

    @staticmethod
    def fillDoneQueue(doneQueue):
        doneQueue.clear()
        codes = Job51Util.getListFromDB("select distinct code from job51")
        doneQueue.update({code[0]: 1 for code in codes})
        logging.info('loadBeenSavedJobInfo: '+str(len(doneQueue))+' successed')

