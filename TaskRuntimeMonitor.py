# coding:utf-8
from abc import abstractmethod, ABCMeta
import threading
import time

class TaskRuntimeMonitor(threading.Thread):
    def __init__(self, taskQueues, allThreads):
        super(TaskRuntimeMonitor, self).__init__()
        self.taskQueues = taskQueues
        self.allThreads = allThreads

    def run(self):
        while True:
            liveThread = 0
            for thread in self.allThreads:
                if thread.isAlive():
                    liveThread += 1
            print "%s \t liveTheads count: %s" % (self.taskQueues.toString(),str(liveThread))
            time.sleep(1)
