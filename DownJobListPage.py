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
import time
import Job51Util
import Job51TaskQueues

#文件保存路径
jobListPath = 'd:/fileloc/jobList'
jobInfoPath = 'd:/fileloc/jobInfo'
#任务队列
queue = Queue()
#已下载 列表
done = {}

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/DownJobPage.log',
                filemode='a')

def createDownJobTaskQueue():
    '''通过 页面访问 确定共有页数 1089 加入线程共享 队列'''
    for i in range(Job51TaskQueues.jobListPageSize,0,-1):
        Job51TaskQueues.jobListPageUrlQueue.put(str(i))

    # for b in os.listdir('D:\\fileloc\\job'):
    #     done[b]=1

class DownJobListPage(threading.Thread):
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
        while not Job51TaskQueues.jobListPageUrlQueue.empty():
            page = Job51TaskQueues.jobListPageUrlQueue.get()
            url='http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=030200%2C00&district=000000&funtype=0100&industrytype=00&issuedate=9&providesalary=08%2C09%2C10%2C11%2C12&keyword=Java&keywordtype=2&curr_page='+str(page)+'&lang=c&stype=1&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=04%2C05%2C06%2C07&lonlat=0%2C0&radius=-1&ord_field=0&list_type=0&dibiaoid=0&confirmdate=9'

            try:
                filename = jobListPath+'/jobListPage'+page+'.html'

                urllib.urlretrieve(url,filename+".tmp")
                os.renames(filename+".tmp",filename)
                Job51TaskQueues.jobListPageQueue.put(filename)
            except Exception as e:
                logging.error(e)
                logging.error('jobListPage'+page+'     down failed url:'+url)

            time.sleep(5)
            logging.info('jobListPage'+page+'     down ok url:'+url)





'''
    def run(self):
        cookie = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie))

        #需要POST的数据#
        postdata=urllib.urlencode({
                # 'keywordtype'   :0,
                # 'jobarea'        : 030200,
                # 'stype'          :1,
                # 'keyword'        :'java',
                # 'image.x'        :43,
                # 'image.y'        :	18,
                #2017-03-12 update by wkai
                'lang':'c',
                'stype':2,
                'postchannel':'0000',
                'fromType':1,
                'confirmdate':9,
                'keywordtype':2,
                'keyword':'Java'
        })

        req = urllib2.Request(
            # url = 'http://search.51job.com/jobsearch/search.html?fromJs=1',
            #2017-03-12 update by wkai
            url='http://search.51job.com/jobsearch/search_result.php?fromJs=1&keyword=Java&keywordtype=2&lang=c&stype=2&postchannel=0000&fromType=1&confirmdate=9',
            data = postdata
        )

        tmp =opener.open(req)
        global queue,rlt
        while not queue.empty():
            page = queue.get()
            # url='http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=000000%2C00&district=000000&funtype=0000&industrytype=00&issuedate=9&providesalary=99&keyword=java&keywordtype=0&curr_page='+str(page)+'&lang=c&stype=2&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&list_type=0&fromType=14&dibiaoid=0&confirmdate=9'
            #url = 'http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=000000%2C00&district=000000&funtype=0000&industrytype=00&issuedate=9&providesalary=99&keyword=%E8%BD%AF%E4%BB%B6&keywordtype=0&curr_page='+str(page)+'&lang=c&stype=2&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&list_type=0&fromType=14&dibiaoid=0&confirmdate=9'
            url='http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=030200%2C00&district=000000&funtype=0100&industrytype=00&issuedate=9&providesalary=08%2C09%2C10%2C11%2C12&keyword=Java&keywordtype=2&curr_page='+str(page)+'&lang=c&stype=1&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=04%2C05%2C06%2C07&lonlat=0%2C0&radius=-1&ord_field=0&list_type=0&dibiaoid=0&confirmdate=9'
            #path = base
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
                        filename = jobListPath+'/'+shortname

                        urllib.urlretrieve(jobUrl,filename+".tmp")
                        os.renames(filename+".tmp",filename)
                        done[shortname]=1
                        count+=1
                    except Exception as e:
                        logging.error(e)
                        logging.error(jobUrl+'     down failed')
                        continue
                    time.sleep(5)
                endtime = datetime.datetime.now()
                logging.info('page:'+str(page)+'    finished down:'+str(count)+'    costtime:'+str((endtime - starttime).seconds)+"s")

            except Exception as e:
                logging.error(e)
                logging.error('page:'+str(page)+'    open failed')
                continue
                '''


if __name__=='__main__':
    createDownJobTaskQueue()
    ths=[]
    for i in range(3):
        t = DownJobListPage()
        t.start()
        ths.append(t)

    for a in ths:
        a.join()


