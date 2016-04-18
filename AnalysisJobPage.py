# coding:utf-8
from Queue import Queue
import gzip
import os
import re
import threading
import StringIO
from bs4 import BeautifulSoup
import cx_Oracle
import datetime
import time
import logging
import zlib

from job51 import job51

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

    i=0
    for i in range(0,length,1):
        filename = jobFiles[i]
        if filename[-4:]=='html':
            queue.put(str(filename))
            i+=1
    logging.info('loadLocalFiles '+str(queue.qsize()))

def loadDoneRecord():
    global done
    #获取已入库列表
    done.clear()
    con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai1")
    cursor = con.cursor()
    cursor.execute("select distinct code from job51")

    codes = cursor.fetchall()
    for code in codes:
        done[code[0]] = 1
    con.close()
    logging.info('loadDoneRecord '+str(len(done)))

def showNum():
    global queue,done, result
    logging.info('file count:'+str(queue.qsize())+' save count:'+str(result.qsize())+' done count:'+str(len(done)) )

class AnalysisJobPage(threading.Thread):
    ''' 解析从51job上下载的html  解析线程'''
    def run(self):
        global queue,done, result

        jobDetailReg = re.compile(r'<span class="label">.*?</span><br>([\s\S]*?)<div class="mt10">')

        while True:
            if queue.empty():
                logging.info('Analysis Task queue is empty sleep 1 min')
                time.sleep(60)
                loadLocalFile()
                loadDoneRecord()
            starttime = datetime.datetime.now()

            shortname = queue.get()
            if shortname in done:
                continue
            filename = base+'/'+shortname
            # html = open(filename, 'r').read().decode('gb2312').encode('utf8')
            soup = BeautifulSoup(open(filename))

            headerTag=soup.find('div',{'class':'tHeader tHjob'})
            contentTag=soup.find('div',{'class':'tCompany_main'})

            try:
                name = headerTag.h1.text
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                name=''

            try:
                addr = headerTag.span.text
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                addr=''

            try:
                salary=headerTag.strong.text
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                salary=''

            try:
                company=headerTag.find('p',{'class':'cname'}).a.text
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                company=''

            try:
                company_info=headerTag.find('p',{'class':'msg ltype'}).text
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                company_info=''

            try:
                year = contentTag.find('em',{'class':'i1'}).next_sibling
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                year=''

            try:
                education = contentTag.find('em',{'class':'i2'}).next_sibling
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                education=''

            try:
                num = contentTag.find('em',{'class':'i3'}).next_sibling
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                num=''

            try:
                release = contentTag.find('em',{'class':'i4'}).next_sibling
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                release=''

            try:
                language = contentTag.find('em',{'class':'i5'}).next_sibling
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                language=''

            try:
                type=contentTag.find('em',{'class':'i6'}).next_sibling
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                type=''

            try:
                welfares=contentTag.find('p',{'class':'t2'}).find_all('span')
                welfare=''
                for tmp in welfares:
                   welfare=welfare+'|'+tmp.text
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                welfare=''
            try:
                # tmpTags = contentTag.find('div',{'class':'bmsg job_msg inbox'}).find_all('p')
                jbDetail = contentTag.find('div',{'class':'bmsg job_msg inbox'}).text.replace("'",' ')
                # jbDetail = ''
                # for p in tmpTags:
                #     if p.attrs.__len__()==0:
                #         jbDetail=jbDetail+p.text
                # jbDetail = ''
                # jbDetail = re.findall(jobDetailReg,open(filename, 'r').read())[0]
                # jbDetail = re.subn(re.compile('<.*?>'), '', jbDetail)[0]#.decode('gb2312').encode('utf8')
            except Exception as e:
                #logging.error(e.message+"===="+filename)
                jbDetail=''


            jobBean=job51(shortname,name, addr, salary, company, company_info, year, education, num, release, language, type, welfare, jbDetail)
            result.put(jobBean)

            endtime = datetime.datetime.now()
            # logging.info(filename+" Analysis costtime: "+str((endtime - starttime).seconds)+"s")
            # done[jobBean.code]=1
            showNum()



class SaveDataToDB(threading.Thread):
    '''分析结果 入库 线程'''
    def run(self):
        logging.info('begin save to DB')
        global result,done
        con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai1")
        cursor = con.cursor()
        jobBean = None
        while True:
            try:
                if result.empty():
                    logging.info('save task is empty sleep 1 min')
                    time.sleep(60)

                jobBean = result.get()
                sql = jobBean.createInsertSql()

                cursor.execute(sql)
                con.commit()

            except Exception as e:
                logging.error(e)
                logging.error('save to db failed:'+ jobBean.code)
                continue
            done[jobBean.code]=1
            showNum()
        con.close()

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



