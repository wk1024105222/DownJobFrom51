# coding:utf-8
from collections import Counter
import jieba
import util

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

    details = util.getListFromDB("select count(1) from job51 t where jbdetail is not null and release in ('06-17发布','06-16发布','06-15发布','06-14发布','06-13发布','06-12发布','06-11发布')")
    print '本次任务合计,'+ str(len(details))
    count = cutWord([d[0].decode('gb2312').encode('utf-8') for d in details])

    for a in count:
        b = a[0]
        if len(b)==1:
            continue
        print b,',',a[1]


