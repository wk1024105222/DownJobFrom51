# coding:utf-8
import os
import urllib
import urllib2
import cookielib
import logging
import time
import MyThread

#文件保存路径
jobListPath = 'd:/fileloc/jobList'
jobInfoPath = 'd:/fileloc/jobInfo'

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/DownJobPage.log',
                filemode='a')

class DownJobInfoPage(MyThread):
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
                logging.info('DownJobInfoPage inQueue is empty wait for '+self.emptyWait+'s')
                time.sleep(self.emptyWait)
                continue
            url = self.inQueue.get()

            try:
                shortname = url[-13:]
                filename = jobInfoPath+'/'+shortname

                urllib.urlretrieve(url,filename+".tmp")
                os.renames(filename+".tmp",filename)
                self.outQueue.put(filename)
            except Exception as e:
                logging.error(e)
                logging.error('jobInfoPage:'+url+'     down failed')
            logging.info('jobInfoPage:'+url+'     down ok ')
            time.sleep(self.requestWait)




