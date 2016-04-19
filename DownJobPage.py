# coding:utf-8
from Queue import Queue
import os
import re
import threading
import urllib
import urllib2
import cookielib
import logging
import datetime
from Job51Util import Job51Util

#文件保存路径
base = 'd:/fileloc/job'
#任务队列
queue = Queue()
#已下载 列表
done = {}

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/DownJobPage.log',
                filemode='a')

class DownJobPage(threading.Thread):
    """
    从51job下载 职位包含java 的job 每个job以html保存本地
    """
    def run(self):
        cookie = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie))

        #需要POST的数据#
        postdata=urllib.urlencode({
                'keywordtype'   :0,
                'jobarea'        : 030200,
                'stype'          :1,
                'keyword'        :'java',
                'image.x'        :43,
                'image.y'        :	18,
        })

        req = urllib2.Request(
            url = 'http://search.51job.com/jobsearch/search.html?fromJs=1',
            data = postdata
        )

        tmp =opener.open(req)
        global queue,rlt
        while not queue.empty():
            page = queue.get()
            # url='http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=000000%2C00&district=000000&funtype=0000&industrytype=00&issuedate=9&providesalary=99&keyword=java&keywordtype=0&curr_page='+str(page)+'&lang=c&stype=2&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&list_type=0&fromType=14&dibiaoid=0&confirmdate=9'
            url = 'http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=000000%2C00&district=000000&funtype=0000&industrytype=00&issuedate=9&providesalary=99&keyword=%E8%BD%AF%E4%BB%B6&keywordtype=0&curr_page='+str(page)+'&lang=c&stype=2&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&list_type=0&fromType=14&dibiaoid=0&confirmdate=9'
            path = base
            starttime = datetime.datetime.now()
            try:
                jobsHtml = opener.open(url).read()

                jobs = re.findall(Job51Util.jobUrlReg,jobsHtml)
                count=0
                for jobUrl in jobs:
                    try:
                        shortname = jobUrl[-13:]
                        if shortname in done:
                            continue
                        filename = path+'/'+shortname

                        urllib.urlretrieve(jobUrl,filename+".tmp")
                        os.renames(filename+".tmp",filename)
                        done[shortname]=1
                        count+=1
                    except Exception as e:
                        logging.error(e)
                        logging.error(jobUrl+'     down failed')
                        continue
                endtime = datetime.datetime.now()
                logging.info('page:'+str(page)+'    finished down:'+str(count)+'    costtime:'+str((endtime - starttime).seconds)+"s")

            except Exception as e:
                logging.error(e)
                logging.error('page:'+str(page)+'    open failed')
                continue

def createDownJobTaskQueue():
    '''通过 页面访问 确定共有页数 1089 加入线程共享 队列'''
    for i in range(1875,0,-1):
        queue.put(str(i))

    for a in os.listdir('D:\\fileloc\\jobname_key_java'):
        done[a]=1
    for b in os.listdir('D:\\fileloc\\job'):
        done[b]=1

if __name__=='__main__':
    createDownJobTaskQueue()
    ths=[]
    for i in range(3):
        t = DownJobPage()
        t.start()
        ths.append(t)

    for a in ths:
        a.join()


