# coding:utf-8
import threading
import time
import cx_Oracle
import time
from functools import wraps
from dbpool import poolOracle

class TaskRuntimeMonitor(threading.Thread):
    """
    driver启动完所有步骤后 启动本线程 循环打印 各个队列长度  线程存活数 以及数据库记录总数
    以观察程序运行情况
    """
    def __init__(self, taskQueues, allThreads):
        super(TaskRuntimeMonitor, self).__init__()
        self.taskQueues = taskQueues
        self.allThreads = allThreads

    def run(self):
        con = poolOracle.connection()
        cursor = con.cursor()
        while True:
            liveThread = 0
            step6 = 0
            step2 = 0
            step3 = 0
            step4 = 0
            step5 = 0

            for thread in self.allThreads['step2']:
                if thread.isAlive():
                    step2 += 1
                    liveThread +=1
            for thread in self.allThreads['step3']:
                if thread.isAlive():
                    step3 += 1
                    liveThread +=1
            for thread in self.allThreads['step4']:
                if thread.isAlive():
                    step4 += 1
                    liveThread +=1
            for thread in self.allThreads['step5']:
                if thread.isAlive():
                    step5 += 1
                    liveThread +=1

            for thread in self.allThreads['step6']:
                if thread.isAlive():
                    step6 += 1
                    liveThread +=1

            if liveThread == 1:
                break

            cursor.execute("select  count(*) from JOB51 t")
            details = cursor.fetchall()

            print "%s\tliveTheads:%s\tstep2:%s\tstep3:%s\tstep4:%s\tstep5:%s\tstep6:%s\tDB record count: %s" % (self.taskQueues.toString(),str(liveThread) ,str(step2),str(step3),str(step4),str(step5),str(step6),str(details[0]))
            time.sleep(1)
        con.close()
        return

def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        """
        注解 计算 函数运行时间
        :param args:
        :param kwargs:
        :return:
        """
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        print ("Total time running %s: %s seconds" % (function.func_name, str(t1-t0)) )
        return result
    return function_timer