# coding:utf-8
import logging
import re
from bs4 import BeautifulSoup
import cx_Oracle
from Entity import job51

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(thread)d [line:%(lineno)d] [%(threadName)s] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log/Job51Util.log',
                filemode='a')

class Job51Util:

    #从页面获取职位url的正则
    jobUrlReg = re.compile(r'href="(http://jobs\.51job\.com/.*?/\d{8}\.html)\?s=0"')

    def getJobInfoFromHtml(self, filename):
        """
        解析html 获取job 信息
        :param filename: 待分析的html文件名
        :return: dict={'jobbean':jobbean, 'urls':urls}
        """
        soup = BeautifulSoup(open(filename),'lxml')

        # print soup.prettify()

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
            jbDetail = contentTag.find('div',{'class':'bmsg job_msg inbox'}).text.replace("'",' ')
        except Exception as e:
            #logging.error(e.message+"===="+filename)
            jbDetail=''

        jobBean=job51(filename[-13:],name, addr, salary, company, company_info, year, education, num, release, language, type, welfare, jbDetail)

        newUrlTags=soup.find_all('a',{'class':'name'})
        urls = [x['href'] for x in newUrlTags]
        logging.info('new urls'+str(len(urls)))
        return {'jobbean':jobBean, 'urls':urls}

    def getListFromDB(self, sql):
        con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai1")
        cursor = con.cursor()
        cursor.execute(sql)
        try:
            result = cursor.fetchall()
        except Exception as e:
            logging.error(e)
            logging.error("getListFromDB %s failed" % (sql))
            result=None
        con.close()
        return result

    def executDMLSql(self, sqls):
        con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai1")
        cursor = con.cursor()
        for sql in sqls:
            try:
                cursor.execute(sql)
                con.commit()
            except Exception as e:
                logging.error(e)
                logging.error("excute sql %s failed" % (sql))
                continue
        con.close()


# Job51Util().getJobInfoFromHtml('D:\\fileloc\\job\\35084712.html')
