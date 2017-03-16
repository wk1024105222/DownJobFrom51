# coding:utf-8
import os
import urllib
import urllib2
import cookielib
import logging
import time
import re
import cx_Oracle
import queues
import base
import driver
import util

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [%(threadName)s] %(filename)s %(module)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/aaa.log',
                filemode='w')

def createDownJobTaskQueue(outQueue):
    '''通过 页面访问 确定共有页数 1089 加入线程共享 队列'''
    for i in range(queues.jobListPageSize,0,-1):
        outQueue.put(str(i))

class DownJobListPage(base.BaseThread):
    """
    从51job下载 职位包含java 的job 每个job以html保存本地
    """
    def run(self):
        cookie = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie))

        #需要POST的数据#
        postdata=urllib.urlencode({
                # 通过火狐 httpfox查看 POST数据  模拟人工访问 2017-03-12 update by wkai
                'lang':'c',
                'stype':2,
                'postchannel':'0000',
                'fromType':1,
                'confirmdate':9,
                'keywordtype':2,
                'keyword':'Java'
        })

        req = urllib2.Request(
            #初始访问地址(全文查询 Java 跳转的地址) 2017-03-12 update by wkai
            url='http://search.51job.com/jobsearch/search_result.php?fromJs=1&keyword=Java&keywordtype=2&lang=c&stype=2&postchannel=0000&fromType=1&confirmdate=9',
            data = postdata
        )

        tmp =opener.open(req)
        while True:
            if self.inQueue.empty():
                # logging.info('DownJobListPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                logging.info('inQueue is empty thread stop')
                break
            page = self.inQueue.get()
            filename = driver.jobListPath+'/jobListPage'+page+'.html'
            if super(DownJobListPage, self).whetherDone('jobListPage'+str(page)):
                logging.info(filename + ' file exists ')
                continue
            url ='http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=000000%2C00&district=000000&funtype=0000&industrytype=00&issuedate=9' \
                 '&providesalary=99&keyword=Java&keywordtype=2&curr_page='+str(page)+'&lang=c&stype=1&postchannel=0000&workyear=99&cotype=99' \
                  '&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&list_type=0&fromType=14&dibiaoid=0&confirmdate=9'

            try:
                urllib.urlretrieve(url,filename+".tmp")
                os.renames(filename+".tmp",filename)
                self.outQueue.put(filename)
                logging.info('[jobListPage:'+page+'] down success url:'+url)
            except Exception as e:
                logging.error(e)
                logging.error('[jobListPage:'+page+'] down failed url:'+url)
            time.sleep(self.requestWait)

        return

    @staticmethod
    def fillInQueue(inQueue):
        createDownJobTaskQueue(inQueue);

    @staticmethod
    def fillDoneQueue(doneQueue):
        for filename in os.listdir(driver.jobListPath):
            doneQueue[filename.split('.')[0]]=1

class AnalysisJobListPage(base.BaseThread):
    def run(self):
        emptyNum=0
        while True:
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

                jobs = re.findall(util.jobUrlReg,jobListPageHtml)
                count=0
                for jobUrl in jobs:
                    self.outQueue.put(jobUrl)
                    count+=1
                logging.info('['+jobListPageFile+ '] Analysis successed getJobInfoUrl:'+str(count))
            except Exception as e:
                logging.error(e)
                logging.error('['+jobListPageFile+'] Analysis failed')
            time.sleep(self.requestWait)
        return

    @staticmethod
    def fillInQueue(inQueue):
        for filename in os.listdir(driver.jobListPath):
            inQueue.put(driver.jobListPath+'/'+filename)

    @staticmethod
    def fillDoneQueue(doneQueue):
        return

class DownJobInfoPage(base.BaseThread):
    """
    从51job下载 职位包含java 的job 每个job以html保存本地
    """
    def run(self):
        cookie = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie))

        #需要POST的数据#
        postdata=urllib.urlencode({
                # 通过火狐 httpfox查看 POST数据  模拟人工访问 2017-03-12 update by wkai
                'lang':'c',
                'stype':2,
                'postchannel':'0000',
                'fromType':1,
                'confirmdate':9,
                'keywordtype':2,
                'keyword':'Java'
        })

        req = urllib2.Request(
            #初始访问地址(全文查询 Java 跳转的地址) 2017-03-12 update by wkai
            url='http://search.51job.com/jobsearch/search_result.php?fromJs=1&keyword=Java&keywordtype=2&lang=c&stype=2&postchannel=0000&fromType=1&confirmdate=9',
            data = postdata
        )

        tmp =opener.open(req)
        emptyNum=0
        while True:
            if self.inQueue.empty():
                if emptyNum>10:
                    logging.info('emptyNum > 10 thread stop')
                    # 连续50次 empty 退出
                    break
                emptyNum+=1
                # logging.info('DownJobInfoPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue

            emptyNum=0
            url = self.inQueue.get()
            shortname = url[-13:]
            filename = driver.jobInfoPath+'/'+shortname
            if super(DownJobInfoPage, self).whetherDone(shortname[0:8]):
                logging.info(filename + ' file exists ')
                continue
            # if os.path.exists(filename) or os.path.exists(filename+".tmp") :
            #     logging.info(filename + ' file exists ')
            #     continue
            try:
                urllib.urlretrieve(url,filename+".tmp")
                os.renames(filename+".tmp",filename)
                self.outQueue.put(filename)
                logging.info('[jobInfoPage:'+shortname+'] down success url:'+url)
            except Exception as e:
                logging.error(e)
                logging.error('[jobInfoPage:'+shortname+'] down failed url:'+url)
            time.sleep(self.requestWait)
        return

    def fillInQueue(inQueue):
        return

    @staticmethod
    def fillDoneQueue(doneQueue):
        for filename in os.listdir(driver.jobInfoPath):
            #不直接使用 是考虑到 有tmp结尾的成功文件
            doneQueue[(filename[0:8])]=1

class AnalysisJobInfoPage(base.BaseThread):
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

            jobBean = util.getJobInfoFromHtml(jobInfoPageFile)['jobbean']
            self.outQueue.put(jobBean)

            logging.info('[jobInfo'+jobBean.code+ '] Analysis successed filepath:'+jobInfoPageFile)
            time.sleep(self.requestWait)
        logging.info('thread is over')
        return

    @staticmethod
    def fillInQueue(inQueue):
        for filename in os.listdir(driver.jobInfoPath):
            # 过滤异常文件
            if not filename.endswith('.err'):
                inQueue.put(driver.jobInfoPath+'/'+filename)

    @staticmethod
    def fillDoneQueue(doneQueue):
        SaveJobInfoToDB.fillDoneQueue(doneQueue)

class SaveJobInfoToDB(base.BaseThread):
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
                filename = driver.jobInfoPath+'/'+jobBean.code+'.html'
                filename_tmp = driver.jobInfoPath+'/'+jobBean.code+'.html.tmp'
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
        codes = util.getListFromDB("select distinct code from job51")
        doneQueue.update({code[0]: 1 for code in codes})
        logging.info('loadBeenSavedJobInfo: '+str(len(doneQueue))+' successed')
