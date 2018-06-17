# coding:utf-8

import uuid

class job51:
    '''实体类'''
    def __init__(self, code, name, addr, salary, company, company_info, year, education, num, release, language, type, welfare, jbDetail,job_type,key_word,addr_detail):
        self.name=name
        self.addr=addr
        self.salary=salary
        self.company=company
        self.company_info=company_info
        self.year=year
        self.education=education
        self.num=num
        self.release=release
        self.language=language
        self.type=type
        self.welfare=welfare
        self.id=uuid.uuid1().hex
        self.code=code
        self.jbDetail=jbDetail
        self.job_type=job_type
        self.key_word=key_word
        self.addr_detail = addr_detail

    def createInsertSql(self):
        '''根据实体类 属性 生成 insert'''
        sql = "insert into job51 (id, code, name, addr, salary, company, company_info, year, education, num, release, language, type, welfare, jbDetail, addr_detail) " \
               "values ('%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
              (self.id,self.code,self.name,self.addr,self.salary,self.company,self.company_info,self.year,self.education,self.num,self.release,self.language,self.type,self.welfare,self.jbDetail,self.addr_detail)
        return sql

    def createUpdateSql(self):
        '''根据实体类 属性 生成 insert'''
        sql = "update job51 set jbDetail ='%s'" % (self.jbDetail)
        return sql

class JobListPage:
    def __init__(self, num, id, url, localpath):
        self.num = num
        self.id = id
        self.url = url
        self.downflag = '0'
        self.localpath = localpath
        self.analyflag = '0'

    def createInsertSql(self):
        return "Insert into JOB51LISTPAGE (ID,URL,DOWNFLAG,LOCALPATH,ANALYFLAG, NUM) values ('%s','%s','%s','%s','%s', %s)" % \
               (self.id,self.url,self.downflag,self.localpath,self.analyflag,self.num)

class JobInfoPage:
    def __init__(self, pageid,url,localpath):

        self.code = url.split('/')[-1][0:-5]
        self.pageid = pageid
        self.url = url
        self.downflag = '0'
        self.localpath = localpath
        self.analyflag = '0'

    def createInsertSql(self):
        return "Insert into JOB51INFOPAGE (CODE,PAGEID,URL,DOWNFLAG,LOCALPATH,ANALYFLAG) values ('%s','%s','%s','%s','%s','%s')" % \
               (self.code,self.pageid,self.url,self.downflag,self.localpath,self.analyflag)
