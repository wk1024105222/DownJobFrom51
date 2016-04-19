# coding:utf-8
from collections import Counter
import jieba
from Job51Util import Job51Util

def handleAddr(addr):
    '''工作地 处理'''
    a = addr.find('-')
    rlt=''

    if a==-1:
        rlt = addr
    else:
        rlt = addr[0:a]
    return rlt

def cutWord(lines):
    """
    结巴分词 统计词频
    :param lines: 待统计字符串列表
    :return:统计结果 [（key,count），（key,count）......]
    """
    c = Counter()
    for i, detail in enumerate(lines):
        seg_list = list(jieba.cut(detail))
        c.update(seg_list)
        if i%1000 == 0:
            print 'No.',str(i),'       finished'
    count = sorted(c.items(), key=lambda x:x[1], reverse=True)

    return count

if __name__=='__main__':
    util = Job51Util()
    details = util.getListFromDB("select lower(name) from job51 t where name like '%java%' ")
    print '本次任务合计,'+ str(len(details))
    count = cutWord([d[0] for d in details])

    for a in count:
        b = a[0]
        if len(b)==1:
            continue
        print b,',',a[1]


