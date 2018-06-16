# coding:utf-8
"""本模块用于 解析51job html """
import logging
import re
from bs4 import BeautifulSoup
import cx_Oracle
import datetime
from lxml import etree
from entity import job51
from dbpool import poolOracle

#从页面获取职位url的正则
#jobUrlReg = re.compile(r'href="(http://jobs\.51job\.com/.*?/\d{8}\.html)\?s=0"')
#2017-03-12 update by wkai
#jobUrlReg = re.compile(r'href="(https://jobs\.51job\.com/.*?/\d{8}\.html)\?s=01&t=')
#2018-06-16 update by wkai https
jobUrlReg = re.compile(r'href="(https://jobs\.51job\.com/.*?/\d{6,12}\.html)\?s=01&t=')

def getJobInfoFromHtml(filename):
    """
    使用BeautifulSoup解析html 获取job 信息
    :param filename: 待分析的html文件名
    :return: dict={'jobbean':jobbean, 'urls':urls}
    jobbean:页面解析出的招聘职位信息
    urls:页面解析出其他的职位连接 可用于递归深度爬取
    """
    try:
        code=filename.split('/')[-1][0:8]
    except Exception as e:
        code=''
    html = open(filename).read().decode('gb2312','ignore')
    soup = BeautifulSoup(html,'lxml')
    # soup = BeautifulSoup(open(filename),'lxml')

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
        company_info=headerTag.find('p',{'class':'msg ltype'}).text.replace('\r',' ').replace('\n',' ').replace('\t',' ').replace('  ',' ')
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
        addr_detail=contentTag.find('div',{'class':'bmsg inbox'}).find('p',{'class':'fp'}).text.replace('\r',' ').replace('\n',' ').replace('\t',' ').replace('  ',' ')
    except Exception as e:
        #logging.error(e.message+"===="+filename)
        addr_detail=''

    jbDetail= ''
    job_type=''
    key_word=''
    try:
        jbDetail = contentTag.find('div',{'class':'bmsg job_msg inbox'}).text.replace("'",' ')
        # print jbDetail
        index2 = jbDetail.find(u'关键字')
        index1 = jbDetail.find(u'职能类别：')
        index3 = jbDetail.index(u'举报')
        if index1>0:
            if index2>0:
                job_type=jbDetail[index1:index2]
                if index3>0:
                    key_word=jbDetail[index2:index3]
                else:
                    key_word=jbDetail[index2]
                jbDetail=jbDetail[:index1]
            else :
                if index3>0:
                    job_type=jbDetail[index1:index3]
                else:
                    job_type=jbDetail[index1:]
                jbDetail=jbDetail[:index1]
        else:
            if index2>0:
                if index3>0:
                    key_word=jbDetail[index2:index3]
                else:
                    key_word=jbDetail[index2]
                jbDetail=jbDetail[:index2]
            else :
                if index3>0:
                    jbDetail=jbDetail[:index3]
                else:
                    pass

    except Exception as e:
        #logging.error(e.message+"===="+filename)
        jbDetail=''

    jobBean=job51(code,name, addr, salary, company, company_info, year, education, num, release, language, type, welfare,
                  jbDetail.replace('\r',' ').replace('\n',' ').replace('\t',' ').replace('  ',' '),
                  job_type.replace('\r',' ').replace('\n',' ').replace('\t',' ').replace('  ',' '),
                  key_word.replace('\r',' ').replace('\n',' ').replace('\t',' ').replace('  ',' '),
                  addr_detail)

    newUrlTags=soup.find_all('a',{'class':'name'})

    urls = []
    try:
        urls = [x['href'] for x in newUrlTags]
    except Exception as e:
        logging.error(e)
    # logging.info('new urls'+str(len(urls)))
    return {'jobbean':jobBean, 'urls':urls}

def getJobDetailInfoFromHtml(filename):
    """
    解析html 获取job 信息
    :param filename: 待分析的html文件名
    :return: dict={'jobbean':jobbean, 'urls':urls}
    """
    try:
        code=filename.split('\\')[-1][:-5]
    except Exception as e:
        code=''

    soup = BeautifulSoup(open(filename),'lxml')
    contentTag=soup.find('div',{'class':'tCompany_main'})

    try:
        jbDetail = contentTag.find('div',{'class':'bmsg job_msg inbox'}).text.replace("'",' ')
        job_type=''
        key_word=''
        # print jbDetail
        index2 = jbDetail.find(u'关键字')
        index1 = jbDetail.find(u'职能类别：')
        index3 = jbDetail.index(u'举报')
        if index1>0:
            if index2>0:
                job_type=jbDetail[index1:index2]
                if index3>0:
                    key_word=jbDetail[index2:index3]
                else:
                    key_word=jbDetail[index2]
                jbDetail=jbDetail[:index1]
            else :
                if index3>0:
                    job_type=jbDetail[index1:index3]
                else:
                    job_type=jbDetail[index1:]
                jbDetail=jbDetail[:index1]
        else:
            if index2>0:
                if index3>0:
                    key_word=jbDetail[index2:index3]
                else:
                    key_word=jbDetail[index2]
                jbDetail=jbDetail[:index2]
            else :
                if index3>0:
                    jbDetail=jbDetail[:index3]
                else:
                    pass

    except Exception as e:
        #logging.error(e.message+"===="+filename)
        jbDetail=''


    return {'jbDetail':jbDetail,'job_type':job_type,'key_word':key_word}

def getJobInfoFromHtmlByXpath(filename):
    """
    使用lxml.etree解析html 获取job 信息
    :param filename: 待分析的html文件名
    :return: dict={'k1':v1, 'k2':v2,......}
    """
    try:
        code=filename.split('\\')[-1][:-5]
    except Exception as e:
        code=''
    html = open(filename).read()
    tree = etree.HTML(html)

    header = tree.xpath('//div[@class="cn"]')[0]

    try:
        name =header.xpath('h1/text()')[0]
    except Exception as e:
        name=''

    try:
        addr = header.xpath('span/text()')[0]
    except Exception as e:
        addr=''

    try:
        salary=header.xpath('strong/text()')[0]
    except Exception as e:
        salary=''

    try:
        company=tree.xpath('//p[@class="cname"]/a/text()')[0]
    except Exception as e:
        company=''

    try:
        company_info=tree.xpath('//p[@class="msg ltype"]/text()')[0]
    except Exception as e:
        company_info=''
    #====================================================================================================
    # try:
    #     year=''
    #     education=''
    #     num=''
    #     release=''
    #     language=''
    #     type=''
    #     tmp1 =tree.xpath('//span[@class="sp4"]/text()')
    #     print '|'.join(tmp1)
    #     for m in tmp1:
    #         if m.find(u'经验')!=-1:
    #             year= m
    #         elif m.find(u'招聘')!=-1:
    #             num=m
    #         elif  m.find(u'发布')!=-1:
    #             release=m
    #         else:
    #             education=m
    #     tmp2 =tree.xpath('//span[@class="sp2"]/text()')
    #     print '|'.join(tmp2)
    # except Exception as e:
    #     type=''

    try:
        year = tree.xpath('//em[@class="i1"]/../text()')[0]
    except Exception as e:
        year=''
    try:
        education = tree.xpath('//em[@class="i2"]/../text()')[0]
    except Exception as e:
        education=''
    try:
        num = tree.xpath('//em[@class="i3"]/../text()')[0]
    except Exception as e:
        num=''
    try:
        release = tree.xpath('//em[@class="i4"]/../text()')[0]
    except Exception as e:
        release=''
    try:
        language = tree.xpath('//em[@class="i5"]/../text()')[0]
    except Exception as e:
        language=''
    try:
        type=tree.xpath('//em[@class="i6"]/../text()')[0]
    except Exception as e:
        type=''

    #====================================================================================================
    try:
        welfare='|'.join([x for x in tree.xpath('//p[@class="t2"]/span/text()')])
    except Exception as e:
        welfare=''

    try:
        jbDetail = '\n'.join([b for b in tree.xpath('//div[@class="bmsg job_msg inbox"]/p/text()')])
    except Exception as e:
        jbDetail=''

    try:
        job_type='|'.join([a for a in tree.xpath('//p[@class="fp f2"][1]/span[@class="el"]/text()')])
    except Exception as e:
        job_type=''

    try:
        key_word='|'.join([c for c in tree.xpath('//p[@class="fp f2"][2]/span[@class="el"]/text()')])
    except Exception as e:
        key_word=''

    job={}
    job['name']=name
    job['addr']=addr
    job['salary']=salary
    job['company']=company
    job['company_info']=company_info
    job['year']=year
    job['education']=education
    job['num']=num
    job['release']=release
    job['language']=language
    job['type']=type
    job['welfare']=welfare
    job['jbDetail']=jbDetail
    job['job_type']=job_type
    job['key_word']=key_word
    return job

def getListFromDB(sql):
    con = poolOracle.connection()
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

def executDMLSql(sqls):
    con = poolOracle.connection()
    cursor = con.cursor()
    for sql in sqls:
        try:
            # cursor.execute(sql.encode('gb2312','ignore'))
            cursor.execute(sql)
            con.commit()
        except Exception as e:
            logging.error(e)
            logging.error("excute sql %s failed" % (sql))

            continue
    con.close()

if __name__ == '__main__':
    getJobInfoFromHtml('D:\\fileloc\\job\\51067847.html')
