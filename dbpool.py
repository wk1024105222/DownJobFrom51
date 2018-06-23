# encoding: utf-8
import cx_Oracle
from DBUtils.PooledDB import PooledDB
import redis
import pymysql;
import dbconfig as Config;

pool = PooledDB(cx_Oracle,
                user = Config.ORACLE_WKAI_USER,
                password = Config.ORACLE_WKAI_PASSWORD,
                dsn = "%s:%s/%s" %(Config.ORACLE_WKAI_HOST,Config.ORACLE_WKAI_PORT,Config.ORACLE_WKAI_DBNAME),
                mincached=Config.DB_MIN_CACHED,
                maxcached=Config.DB_MAX_CACHED,
                maxshared=Config.DB_MAX_SHARED,
                maxconnections=Config.DB_MAX_CONNECYIONS)

# pool = PooledDB(creator=pymysql, mincached=Config.DB_MIN_CACHED , maxcached=Config.DB_MAX_CACHED,
#                                    maxshared=Config.DB_MAX_SHARED, maxconnections=Config.DB_MAX_CONNECYIONS,
#                                    blocking=Config.DB_BLOCKING, maxusage=Config.DB_MAX_USAGE,
#                                    setsession=Config.DB_SET_SESSION,
#                                    host=Config.MYSQL_WKAI_HOST , port=Config.MYSQL_WKAI_PORT ,
#                                    user=Config.MYSQL_WKAI_USER , passwd=Config.MYSQL_WKAI_PASSWORD ,
#                                    db=Config.MYSQL_WKAI_DBNAME , use_unicode=False, charset=Config.DB_CHARSET);

poolRedis  = redis.ConnectionPool(host='1.1.1.102', port=6379)

# con = pool.connection()
# cursor = con.cursor()
# cursor.execute("select * from stockinfo")
# for code in cursor.fetchall():
#     print code[0]
# cursor.close()
# con.close()


