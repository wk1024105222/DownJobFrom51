import threading

__author__ = 'wkai'

class MyThread(threading.Thread):
    """
    从51job下载 职位包含java 的job 每个job以html保存本地
    """
    def __init__(self, inQueue, outQueue):
        threading.Thread.__init__(self)
        self.inQueue = inQueue
        self.outQueue = outQueue
