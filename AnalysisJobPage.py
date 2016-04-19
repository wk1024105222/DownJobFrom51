# coding:utf-8
from Queue import Queue
import os
import threading
import time
import logging

from Job51Util import Job51Util

#文件保存路径
base = 'd:/fileloc/job'
#任务队列
queue = Queue()
#已完成列表
done = {}
result = Queue()

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/AnalysisJobPage.log',
                filemode='a')

def loadLocalFile():
    global queue
    jobFiles = os.listdir(base)
    length = len(jobFiles)
    for i in range(0,length,1):
        filename = jobFiles[i]
        if filename[-4:]=='html':
            queue.put(str(filename))
    logging.info('loadLocalFiles '+str(queue.qsize()))

def loadDoneRecord():
    global done
    done.clear()
    util = Job51Util()
    codes = util.getListFromDB("select distinct code from job51")
    done.update({code[0]: 1 for code in codes})
    logging.info('loadDoneRecord '+str(len(done)))

def showNum():
    global queue,done, result
    logging.info('file count:'+str(queue.qsize())+' save count:'+str(result.qsize())+' done count:'+str(len(done)) )

class AnalysisJobPage(threading.Thread):
    ''' 解析从51job上下载的html  解析线程'''
    def run(self):
        util = Job51Util()
        global queue,done, result

        while True:
            if queue.empty():
                logging.info('Analysis Task queue is empty sleep 1 min')
                time.sleep(60)
                loadLocalFile()
                loadDoneRecord()

            shortname = queue.get()
            if shortname in done:
                continue
            result.put(util.getJobInfoFromHtml(base+'/'+shortname)['jobbean'])

            showNum()

class SaveDataToDB(threading.Thread):
    '''分析结果 入库 线程'''
    def run(self):
        logging.info('begin save to DB')
        global result,done
        jobBean = None
        sqls=[]
        count=0
        util = Job51Util()
        while True:
            if result.empty():
                logging.info('save task is empty sleep 1 min')
                time.sleep(60)

            jobBean = result.get()
            sqls.append( jobBean.createInsertSql())
            count+=1
            if count==100:
                util.executDMLSql(sqls)
                sqls=[]
                count=0
            showNum()

if __name__=='__main__':
    loadLocalFile()
    loadDoneRecord()
    threads = []
    for i in range(1):
        t = AnalysisJobPage()
        t.start()
        threads.append(t)

    b = SaveDataToDB()
    b.start()
    threads.append(b)

    for a in threads:
        a.join()



