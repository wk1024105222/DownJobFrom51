# python原生实现爬虫集合

Job51:从前程无忧下载招聘信息 预期通过分词 分析程序员热门技能
version:0c20657c57fcad19533cf12072d0499905bf5aaf   V1.0版本  实现6个操作步骤 多线程 运行  通过Queue做为任务队列  测试通过
version:ca09fe32563f4bd77e2d618c3de86b05c6a32a88   V2.0版本 在1.0基础上 将任务队列抽象为抽象类BaseQueue  新建51job的任务队列Job51TaskQueue(BaseQueue)
version:b1025b001b5eb816bcda28da806ae9cda229e81b   V2.1版本 在2.0基础上 实现支持多进程运行  但存在遗留问题 后续需要将已完成任务 也放到共享队列中
version:835fa3891d0efc3827365c90eeca42e1cbe2f162   V2.2版本 20180616在2.0基础上修正参数，测试可用版本 1、任务启动 从本地文件读 待处理、已处理任务 2、使用Python Queue做消息队列

gzlibrary:使用Scrapy从广州图书馆下载全部书籍信息158W 每种书籍的馆藏信息可以从 http 返回的Json中解析到 情景不适合使用Scrapy