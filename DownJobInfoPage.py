# coding:utf-8
import os
import urllib
import urllib2
import cookielib
import logging
import time
import MyThread
import Job51Driver
import AnalysisJobInfoPage

class DownJobInfoPage(MyThread.MyThread):
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
            filename = Job51Driver.jobInfoPath+'/'+shortname
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
        for filename in os.listdir(Job51Driver.jobInfoPath):
            #不直接使用 是考虑到 有tmp结尾的成功文件
            doneQueue[(filename[0:8])]=1





