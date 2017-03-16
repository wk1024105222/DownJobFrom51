# coding:utf-8
import threading
import time
import cx_Oracle
import time
from functools import wraps
from dbpool import pool

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
        con = pool.connection()
        cursor = con.cursor()
        while True:
            liveThread = 0
            for thread in self.allThreads:
                if thread.isAlive():
                    liveThread += 1
            if liveThread == 1:
                break

            cursor.execute("select  count(*) from JOB51 t")
            details = cursor.fetchall()

            print "%s \t liveTheads count: %s  DB record count: %s" % (self.taskQueues.toString(),str(liveThread), str(details[0]))
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