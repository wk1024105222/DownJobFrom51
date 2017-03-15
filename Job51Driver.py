import Job51TaskQueues
import DownJobListPage

if __name__=='__main__':
    DownJobListPage.createDownJobTaskQueue(Job51TaskQueues.queues['12']);

    allThreads = []
    for i in range(1):
        step2 = DownJobListPage(Job51TaskQueues.queues['12'],Job51TaskQueues.queues['23'])
        step2.start()
        allThreads.append(step2)

    for i in range(1):
        step3 = DownJobListPage(Job51TaskQueues.queues['23'],Job51TaskQueues.queues['34'])
        step3.start()
        allThreads.append(step3)

    for i in range(1):
        step4 = DownJobListPage(Job51TaskQueues.queues['34'],Job51TaskQueues.queues['45'])
        step4.start()
        allThreads.append(step4)

    for i in range(1):
        step5 = DownJobListPage(Job51TaskQueues.queues['45'],None)
        step5.start()
        allThreads.append(step5)
