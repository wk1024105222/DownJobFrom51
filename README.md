# DownJobFrom51
down jobinfo from job51 by python

version:0c20657c57fcad19533cf12072d0499905bf5aaf   V1.0版本  实现6个操作步骤 多线程 运行  通过Queue做为任务队列  测试通过
version:ca09fe32563f4bd77e2d618c3de86b05c6a32a88   V2.0版本 在1.0基础上 将任务队列抽象为抽象类BaseQueue  新建51job的任务队列Job51TaskQueue(BaseQueue)
version:b1025b001b5eb816bcda28da806ae9cda229e81b   V2.1版本 在2.0基础上 实现支持多进程运行  但存在遗留问题 后续需要将已完成任务 也放到共享队列中