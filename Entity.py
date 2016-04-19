# coding:utf-8

import uuid

class job51:
    '''实体类'''
    def __init__(self, shortname, name, addr, salary, company, company_info, year, education, num, release, language, type, welfare, jbDetail):
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
        self.code=shortname
        self.jbDetail=jbDetail

    def createInsertSql(self):
        '''根据实体类 属性 生成 insert'''
        sql = "insert into job51 (id, code, name, addr, salary, company, company_info, year, education, num, release, language, type, welfare, jbDetail) " \
               "values ('%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
              (self.id,self.code,self.name,self.addr,self.salary,self.company,self.company_info,self.year,self.education,self.num,self.release,self.language,self.type,self.welfare,self.jbDetail)
        return sql

    def createUpdateSql(self):
        '''根据实体类 属性 生成 insert'''
        sql = "update job51 set jbDetail ='%s'" % (self.jbDetail)
        return sql


