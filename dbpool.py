# encoding: utf-8
import cx_Oracle
from DBUtils.PooledDB import PooledDB
import redis

poolOracle = PooledDB(cx_Oracle, user = "wkai", password = "wkai", dsn = "127.0.0.1:1521/wkai",mincached=2,maxcached=33,maxshared=33,maxconnections=2)

poolRedis = pool = redis.ConnectionPool(host='1.1.1.102', port=6379)