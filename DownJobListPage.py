# coding:utf-8
import os
import urllib
import urllib2
import cookielib
import logging
import time
import Job51TaskQueues
import MyThread
import Job51Driver

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [%(threadName)s] %(filename)s %(module)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/DownJobListPage.log',
                filemode='a')

def createDownJobTaskQueue(outQueue):
    '''通过 页面访问 确定共有页数 1089 加入线程共享 队列'''
    for i in range(Job51TaskQueues.jobListPageSize,0,-1):
        outQueue.put(str(i))

    # for b in os.listdir('D:\\fileloc\\job'):
    #     done[b]=1

class DownJobListPage(MyThread.MyThread):
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
            time.sleep(self.requestWait)
            if self.inQueue.empty():
                # logging.info('DownJobListPage inQueue is empty wait for '+str(self.emptyWait)+'s')
                time.sleep(self.emptyWait)
                logging.info('inQueue is empty thread stop')
                break
            page = self.inQueue.get()
            # url='http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=030200%2C00&district=000000&funtype=0100&industrytype=00&issuedate=9' \
            #     '&providesalary=08%2C09%2C10%2C11%2C12&keyword=Java&keywordtype=2&curr_page='+str(page)+'&lang=c&stype=1&postchannel=0000&workyear=99&cotype=99' \
            #      '&degreefrom=99&jobterm=99&companysize=04%2C05%2C06%2C07&lonlat=0%2C0&radius=-1&ord_field=0&list_type=0&dibiaoid=0&confirmdate=9'
            url ='http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=000000%2C00&district=000000&funtype=0000&industrytype=00&issuedate=9' \
                 '&providesalary=99&keyword=Java&keywordtype=2&curr_page='+str(page)+'&lang=c&stype=1&postchannel=0000&workyear=99&cotype=99' \
                  '&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&list_type=0&fromType=14&dibiaoid=0&confirmdate=9'

            try:
                filename = Job51Driver.jobListPath+'/jobListPage'+page+'.html'

                urllib.urlretrieve(url,filename+".tmp")
                os.renames(filename+".tmp",filename)
                self.outQueue.put(filename)
                logging.info('[jobListPage:'+page+'] down success url:'+url)
            except Exception as e:
                logging.error(e)
                logging.error('[jobListPage:'+page+'] down failed url:'+url)

        return

    def fillInQueue(self):
        createDownJobTaskQueue(self.inQueue);

if __name__=='__main__':
    createDownJobTaskQueue()
    ths=[]
    for i in range(3):
        t = DownJobListPage()
        t.start()
        ths.append(t)

    for a in ths:
        a.join()


