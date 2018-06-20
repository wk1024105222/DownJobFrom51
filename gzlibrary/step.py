# coding:utf-8
"""本模块 包含任务过程中 涉及的所有步骤"""
import os
import urllib
import urllib2
import cookielib
import logging
import time
from crawler import base
from crawler.dbpool import poolOracle
from entity import *
import json

holdbasepath = 'D:/fileloc/gzlib/hold'

class DownBookHoldJson(base.BaseThread):
    """
    下载书籍馆藏数据  http://opac.gzlib.gov.cn/opac/api/holding/+bookid 返回Json数据
    """
    def run(self):
        while True:
            if self.inQueue.empty():
                logging.info('DownBookHoldJson inQueue is empty wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)

            code = None
            try:
                code = self.inQueue.get()
            except Exception as e:
                logging.error(e)
                continue

            if code == None:
                logging.info('DownBookHoldJson get null from inQueue wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue

            url = 'http://opac.gzlib.gov.cn/opac/api/holding/%s' % (code)
            filepath = '%s/%s.json' % (holdbasepath, code)

            updatesql = "update GZLIBBOOKINFO set holddownflag='1' where id = '%s'" % (code)

            if os.path.exists(filepath):
                self.dbQueue.put(updatesql)
                logging.info(filepath + ' file exists ')
                continue

            try:
                urllib.urlretrieve(url,filepath+".tmp")
                os.renames(filepath+".tmp",filepath)
                self.dbQueue.put(updatesql)
                self.outQueue.put(filepath)
                logging.info('[%s.json] down success url:%s' % (code,url))
            except Exception as e:
                logging.error(e)
                logging.error('[%s.json] down failed url:%s' % (code,url))
            time.sleep(self.requestWait)
        return

    @staticmethod
    def fillInQueue(inQueue):
        con = poolOracle.connection()
        cursor = con.cursor()
        cursor.execute("select id from gzlibbookinfo where holddownflag='0'")
        for code in cursor.fetchall():
            inQueue.put(code[0])
        con.close()

    @staticmethod
    def fillDoneQueue(doneQueue):
        pass

class AnalysisBookHoldJson(base.BaseThread):
    """
    解析书籍馆藏信息Json
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
                    logging.info('AnalysisBookHoldJson inQueue is empty wait for '+str(self.emptyWait)+'s')
                    time.sleep(self.emptyWait)
                    continue
            # 计数归零
            emptyNum=0

            code = None
            try:
                code = self.inQueue.get()
            except Exception as e:
                logging.error(e)

            if code == None:
                logging.info('AnalysisBookHoldJson get null task from inQueue wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                continue
            filepath = '%s/%s.json' % (holdbasepath, code)

            try:
                f = open(filepath)
                booklist = json.load(f)['holdingList']

                for book in booklist:
                    bookHold = BookHold(book['barcode'],book['callno'],book['orglib'],book['orglocal'],book['cirtype'],
                                        book['totalLoanNum'],book['totalRenewNum'],book['shelfno'],code)
                    insertsql = bookHold.createInsertSql()
                    self.dbQueue.put(insertsql)
                updatesql = "update GZLIBBOOKINFO set holdanalyflag='1' where id = '%s'" % (code)
                self.dbQueue.put(updatesql)
                logging.info('[%s] Analysis successed ' % (filepath))
            except Exception as e:
                logging.error(e)
                logging.error('[%s] Analysis failed  ' % (filepath))
                updatesql = "update GZLIBBOOKINFO set holddownflag='0',holdanalyflag='0'  where id = '%s'" % (code)
                self.dbQueue.put(updatesql)
        return

    @staticmethod
    def fillInQueue(inQueue):
        con = poolOracle.connection()
        cursor = con.cursor()
        cursor.execute("select id from gzlibbookinfo where holddownflag='1' and holdanalyflag='0'")
        for code in cursor.fetchall():
            inQueue.put(code[0])
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
        # fp = open("allsql.script","w")
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
                cursor.execute(sql.encode('gb2312','ignore'))
                con.commit()
            except Exception as e:
                logging.info(sql)
                logging.info(e)
        con.close()
        logging.info('thread is over')
        return

    @staticmethod
    def fillInQueue(inQueue):
        return

    @staticmethod
    def fillDoneQueue(doneQueue):
        return
