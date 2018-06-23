#! /usr/bin/env python
import cx_Oracle


def get_ora_conn(ip, port, sid, user, password):
    dsn = cx_Oracle.makedsn(ip, port, sid)
    conn = cx_Oracle.connect(user, password, dsn)
    return conn


def test():
    conn = get_ora_conn("127.0.0.1", 1521, "aaaa", "aaaa", "aaaa")
    table_schema = "AAAA"
    tabs = get_tables(conn, table_schema)
    for tab in tabs:
        get_table_ddl(conn, table_schema, tab[0])


def get_table_ddl(db_conn, table_schema, table_name):
    cols, inds, cons = get_table_meta(db_conn, table_schema, table_name)
    gene_mysql_ddl(table_name, cols, inds, cons)


def get_tables(db_conn, table_schema):
    sql = "select table_name from dba_tables where owner=:owner"
    cur = db_conn.cursor()
    cur.execute(sql, {"owner": table_schema})
    tabs = cur.fetchall()
    return tabs


def get_table_meta(db_conn, table_schema, table_name):
    sql_col = """ 
        select table_name, column_name , data_type, data_length, data_precision, data_scale, nullable, data_default  
        from dba_tab_columns  
        where owner = :owner 
        and table_name = :table_name 
        order by column_id 
    """
    cur = db_conn.cursor()
    cur.execute(sql_col, {'owner': table_schema, 'table_name': table_name})
    tab_cols = cur.fetchall()
    inds = get_table_indexes(db_conn, table_schema, table_name)
    cons = get_table_pkuk(db_conn, table_schema, table_name)
    return tab_cols, inds, cons

def get_table_pkuk(db_conn, table_schema, table_name):
    sql = """ 
        select constraint_name, CONSTRAINT_TYPE   
        from dba_constraints  
        where owner = :owner 
        and table_name = :table_name 
        and constraint_type in ('P', 'U') 
    """

    sql2 = """ 
        select column_name  
        from dba_cons_columns  
        where owner = :owner 
        and table_name = :table_name 
        and constraint_name = :constraint_name 
        order by position 
    """

    cur = db_conn.cursor()
    cur.execute(sql, {'owner': table_schema, 'table_name': table_name})
    cons = cur.fetchall()
    ret = []
    for cons_name, cons_type in cons:
        cur.execute(sql2, {'owner': table_schema, 'table_name': table_name, 'constraint_name': cons_name})
        cons_cols = cur.fetchall()
        cons_expr = ",".join(i[0] for i in cons_cols)
        ret.append([cons_name, cons_type, cons_expr])
    return ret

def get_table_indexes(db_conn, table_schema, table_name):
    sql = """ 
        select index_name, index_type, uniqueness from dba_indexes where owner = :owner and table_name = :table_name 
    """

    sql2 = """ 
        select column_name 
        from dba_ind_columns  
        where index_owner = :index_owner  
        and index_name = :index_name 
        and table_owner = :table_owner 
        and table_name = :table_name  
        order by COLUMN_POSITION 
    """

    cur = db_conn.cursor()
    cur.execute(sql, {'owner': table_schema, 'table_name': table_name})
    indexes = cur.fetchall()
    ret = []
    for index_name, index_type, uniqueness in indexes:
        cur.execute(sql2, {'table_owner': table_schema, 'index_owner': table_schema, 'table_name': table_name,
                           'index_name': index_name})
        ind_cols = cur.fetchall()
        ind_exp = ",".join((i[0] for i in ind_cols))
        ret.append([index_name, index_type, uniqueness, ind_exp])
    return ret
    # FBI
    # DBA_IND_EXPRESSIONS

def get_table_comments(db_conn, table_schema, table_name):
    sql_tab_comments = "select comment from dba_tab_comments where owner = :owner and table_name = :table_name"
    sql_col_comments = "select column_name, comment from dba_col_comments where owner = :owner and table_name = :table_name"

def gene_mysql_ddl(table_name, tab_cols, inds, cons):
    ddl = "create table %s (" % table_name
    sep = "\n    "
    for c in tab_cols:
        col = map_col_type(c)
        ddl = ddl + sep + col
        sep = ",\n    "

    added_inds = {}

    for cons_name, cons_type, cons_expr in cons:
        if cons_type == 'P':
            ddl = ddl + sep + "primary key (%s)" % cons_expr
            added_inds[cons_expr] = 1
        elif cons_type == 'U':
            ddl = ddl + sep + "unique key (%s)" % cons_expr
            added_inds[cons_expr] = 1

    for index_name, index_type, uniqueness, index_exp in inds:
        if index_exp in added_inds:
            continue
        if uniqueness == 'UNIQUE':
            ddl = ddl + sep + "unique key %s( %s )" % (index_name, index_exp)
        else:
            ddl = ddl + sep + "key %s( %s )" % (index_name, index_exp)

    ddl = ddl + " \n) engine=innodb default charset=utf8;"
    print ddl
    # type mapping: number


def map_col_type(c):
    (table_name, column_name, data_type, data_length, data_precision, data_scale, nullable, data_default) = c
    if data_type == 'NUMBER':
        if data_precision != None and data_scale != None:
            if data_scale > 30:
                data_scale = 30
            mapped_col = "decimal(%d,%d)" % (data_precision, data_scale)
        elif data_scale != None:
            if data_scale > 30:
                data_scale = 30
            mapped_col = "decimal(38,%d)" % (data_scale)
        else:
            mapped_col = "bigint"
    elif data_type == 'VARCHAR2' or data_type == 'VARCHAR':
        mapped_col = "varchar(%d)" % data_length
    elif data_type == 'CLOB':
        mapped_col = 'text'
    elif data_type == 'DATE':
        mapped_col = 'datetime'
    elif data_type == 'BLOB':
        mapped_col = 'binary'
    elif data_type == 'CHAR':
        mapped_col = 'char(%d)' % data_length
    else:
        mapped_col = data_type

    if nullable == 'N':
        mapped_col = mapped_col + ' not null'
    if data_default != None:
        mapped_col = mapped_col + " default '" + data_default + "'"

    return "%s %s" % (column_name, mapped_col)


if __name__ == "__main__":
    test()