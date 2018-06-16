# coding:utf-8
"""本模块 包含任务过程中 涉及的所有步骤"""
import os
import urllib
import urllib2
import cookielib
import logging
import time
import re
import queues
import base
import driver
import util
from dbpool import poolOracle
from entity import *



def createDownJobTaskQueue(outQueue):
    '''通过 页面访问 确定总页数 加入线程共享 队列'''
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
            url='http://search.51job.com/jobsearch/search_result.php?fromJs=1&keyword=Java&keywordtype=2'
                '&lang=c&stype=2&postchannel=0000&fromType=1&confirmdate=9',
            data = postdata
        )
        tmp =opener.open(req)

        while True:
            if self.inQueue.empty():
                logging.info('DownJobListPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)

            page = None
            try:
                page = self.inQueue.get()
            except Exception as e:
                logging.error(e)
                continue

            if page == None:
                logging.info('DownJobListPage get null from inQueue wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue
            pagenum = page[0]
            pageid = page[1]
            pageurl = page[2]
            pagelocalpath = page[3]

            updatesql = "update job51listpage set downflag='1' where id = '%s'" % (pageid)

            if os.path.exists(pagelocalpath):
                self.dbQueue.put(updatesql)
                logging.info(pagelocalpath + ' file exists ')
                continue

            try:
                urllib.urlretrieve(pageurl,pagelocalpath+".tmp")
                os.renames(pagelocalpath+".tmp",pagelocalpath)
                self.dbQueue.put(updatesql)
                self.outQueue.put(pagelocalpath)
                logging.info('[%s] down success url:%s' % (pageid,pageurl))
            except Exception as e:
                logging.error(e)
                logging.error('[%s] down failed url:%s' % (pageid,pageurl))
            time.sleep(self.requestWait)
        return

    @staticmethod
    def fillInQueue(inQueue):
        con = poolOracle.connection()
        cursor = con.cursor()
        cursor.execute("select num,id,url, localpath from job51listpage where downflag='0' order by num")
        for listpage in cursor.fetchall():
            inQueue.put(listpage)
        con.close()

    @staticmethod
    def fillDoneQueue(doneQueue):
        pass

class AnalysisJobListPage(base.BaseThread):
    """
    解析查询结果页面 获取所有招聘信息url
    """
    def run(self):
        emptyNum=0
        while True:
            if self.inQueue.empty():
                if emptyNum>10:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                else :
                    emptyNum+=1
                    logging.info('AnalysisJobListPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                    time.sleep(self.emptyWait)
                    continue
            # 计数归零
            emptyNum=0

            page = None
            try:
                page = self.inQueue.get()
            except Exception as e:
                logging.error(e)

            if page == None:
                logging.info('AnalysisJobListPage get null task from inQueue wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue

            pagenum = page[0]
            pageid = page[1]
            pageurl = page[2]
            pagelocalpath = page[3]

            try:
                jobListPageHtml = open(pagelocalpath, 'rb').read()
                joburls = re.findall(util.jobUrlReg,jobListPageHtml)

                # print '%s %d' % (jobListPageFile,len(jobs))
                for jobUrl in joburls:
                    self.outQueue.put(jobUrl)
                    insertsql = JobInfoPage(pageid, jobUrl,'%s/%s' % (driver.jobInfoPath, jobUrl.split('/')[-1])).createInsertSql()
                    self.dbQueue.put(insertsql)

                updatesql = "update job51listpage set analyflag='1' where id = '%s'" % (pageid)
                self.dbQueue.put(updatesql)
                logging.info('['+pagelocalpath+ '] Analysis successed getJobInfoUrl:'+str(len(joburls)))
            except Exception as e:
                logging.error(e)
                logging.error('['+pagelocalpath+'] Analysis failed')
        return

    @staticmethod
    def fillInQueue(inQueue):
        con = poolOracle.connection()
        cursor = con.cursor()
        cursor.execute("select num,id,url, localpath from job51listpage where downflag='1' and analyflag='0' order by num")
        for listpage in cursor.fetchall():
             inQueue.put(listpage)
        con.close()

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
            url='http://search.51job.com/jobsearch/search_result.php?fromJs=1&keyword=Java&keywordtype=2'
                '&lang=c&stype=2&postchannel=0000&fromType=1&confirmdate=9',
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
                else:
                    emptyNum+=1
                    logging.info('DownJobInfoPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                    time.sleep(self.emptyWait)
                    continue
            emptyNum=0

            infopage = None
            try:
                infopage = self.inQueue.get()
            except Exception as e:
                logging.error(e)

            if infopage == None:
                logging.info('DownJobInfoPage get null task from inQueue wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue

            pagecode = infopage[0]
            pageid = infopage[1]
            pageurl = infopage[2]
            pagelocalpath = infopage[3]

            updatesql = "update job51infopage set downflag='1' where code = '%s'" % (pagecode)

            if os.path.exists(pagelocalpath):
                self.dbQueue.put(updatesql)
                logging.info(pagelocalpath + ' file exists ')
                continue

            try:
                urllib.urlretrieve(pageurl,pagelocalpath+".tmp")
                os.renames(pagelocalpath+".tmp",pagelocalpath)
                self.dbQueue.put(updatesql)
                self.outQueue.put(pagelocalpath)
                logging.info('[%s] down success %s:' % (pagelocalpath,pageurl))
                # print '[jobInfoPage:'+shortname+'] down success url:'+url
            except Exception as e:
                logging.error(e)
                logging.error('[%s] down failed %s:' % (pagelocalpath,pageurl))
                print '[%s] down failed %s:' % (pagelocalpath,pageurl)
        return

    @staticmethod
    def fillInQueue(inQueue):
        con = poolOracle.connection()
        cursor = con.cursor()
        cursor.execute("select code,pageid,url,localpath from job51infopage where downflag='0'")
        for infopage in cursor.fetchall():
             inQueue.put(infopage)
        con.close()

    @staticmethod
    def fillDoneQueue(doneQueue):
        return

class AnalysisJobInfoPage(base.BaseThread):
    """
    解析每条招聘信息的Html
    """
    def run(self):
        emptyNum=0
        while True:
            if self.inQueue.empty():
                if emptyNum>50:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                else:
                    emptyNum+=1
                    logging.info('AnalysisJobInfoPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                    time.sleep(self.emptyWait)
                    continue
            emptyNum=0

            infopage = None
            try:
                infopage = self.inQueue.get()
            except Exception as e:
                logging.error(e)

            if infopage == None:
                logging.info('AnalysisJobInfoPage get null task from inQueue wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue

            pagecode = infopage[0]
            pageid = infopage[1]
            pageurl = infopage[2]
            pagelocalpath = infopage[3]

            if not os.path.exists(pagelocalpath):
                logging.info(pagelocalpath + ' file not exists ')
                continue

            jobBean = util.getJobInfoFromHtml(pagelocalpath)['jobbean']
            self.dbQueue.put(jobBean.createInsertSql())

            updatesql = "update job51infopage set analyflag='1' where code = '%s'" % (pagecode)
            self.dbQueue.put(updatesql)

            logging.info('[jobInfo:%s] Analysis successed filepath:%s' % (pagecode,pagelocalpath))
            time.sleep(self.requestWait)
        logging.info('thread is over')
        return

    @staticmethod
    def fillInQueue(inQueue):
        con = poolOracle.connection()
        cursor = con.cursor()
        cursor.execute("select code,pageid,url,localpath from job51infopage where downflag='1' and analyflag='0'")
        for infopage in cursor.fetchall():
             inQueue.put(infopage)
        con.close()

    @staticmethod
    def fillDoneQueue(doneQueue):
        return

class DBExecuter(base.BaseThread):
    """
    数据入库
    """
    def run(self):
        con = poolOracle.connection()
        cursor = con.cursor()
        emptyNum=0
        fp = open("allsql.sql","w")
        while True:
            if self.inQueue.empty():
                if emptyNum>50:
                    # 连续50次 empty 退出
                    logging.info('emptyNum > 10 thread stop')
                    break
                else:
                    emptyNum+=1
                    logging.info('SaveJobInfoToDB inQueue is empty wait for '+str(self.emptyWait)+'s emptyNum:'+str(emptyNum))
                    time.sleep(self.emptyWait)
                    continue
            emptyNum=0

            sql = None
            try:
                sql = self.inQueue.get()
            except Exception as e:
                logging.error(e)

            if sql == None:
                logging.info('SaveJobInfoToDB get null task from inQueue wait for '+str(self.emptyWait)+'s emptyNum:'+str(emptyNum))
                time.sleep(self.emptyWait)
                continue

            try:
                fp.write(sql.encode('gb2312','ignore'))
                cursor.execute(sql.encode('gb2312','ignore'))
                print sql
                con.commit()
            except Exception as e:
                logging.error(e)
        con.close()
        fp.closed()
        logging.info('thread is over')
        return

    @staticmethod
    def fillInQueue(inQueue):
        return

    @staticmethod
    def fillDoneQueue(doneQueue):
        return
