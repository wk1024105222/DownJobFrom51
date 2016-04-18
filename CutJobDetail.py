# coding:utf-8
from collections import Counter
import cx_Oracle
import datetime
import jieba

'''结巴分词 统计词频'''

def handleAddr(addr):
    '''工作地 处理'''
    a = addr.find('-')
    rlt=''

    if a==-1:
        rlt = addr
    else:
        rlt = addr[0:a]
    return rlt

con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai1")
cursor = con.cursor()
cursor.execute("select lower(company_info) from job51 t where company_info is not null ")

details = cursor.fetchall()
con.close()

c = Counter()
print '本次任务合计,'+ str(len(details))
i=0
starttime = datetime.datetime.now()
for d in details:
    # print d[0].split('|')[0].strip().replace('？','')
    # seg_list = list(jieba.cut(handleAddr(d[0])))
    seg_list = list(jieba.cut(d[0]))
    c.update(seg_list)
    i+=1
    if i%1000 == 0:
        print 'No.',str(i),'       finished'

count = sorted(c.items(), key=lambda x:x[1], reverse=True)
endtime = datetime.datetime.now()
print "耗时,"+str((endtime - starttime).seconds)+"s"
for a in count:
    b = a[0].encode('utf8')
    if len(b)==1:
        continue
    print b,',',a[1]


