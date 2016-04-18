# coding:utf-8

import cx_Oracle
import time

'''查看 每分钟入库数据量'''

con = cx_Oracle.connect("wkai/wkai@127.0.0.1/wkai1")
cursor = con.cursor()

while True:
    cursor.execute("select  (count(1)-60725)/((sysdate- to_date('2016-04-17 23:50:51','yyyy-mm-dd hh24:mi:ss'))*24*60) from JOB51 t")
    details = cursor.fetchall()
    print details[0]
    time.sleep(60)
con.close()
