# coding:utf-8

class BookHold:
    def __init__(self, id,callno,orglib,orgarea,cirtype,borrownum,renewnum,shelfno,bookcode):
        self.id = id
        self.callno = callno
        self.orglib = orglib
        self.orgarea = orgarea
        self.cirtype = cirtype
        self.borrownum = borrownum
        self.renewnum = renewnum
        self.shelfno = shelfno
        self.bookcode = bookcode

    def createInsertSql(self):
        return "INSERT INTO GZLIBBOOKHOLD (ID, CALLNO, ORGLIB, ORGAREA, CIRTYPE, BORROWNUM, RENEWNUM, SHELFNO, BOOKCODE) VALUES " \
               "('%s', '%s', '%s', '%s', '%s', %s, %s, '%s', '%s')" % \
               (self.id,self.callno,self.orglib,self.orgarea,self.cirtype,self.borrownum,self.renewnum,self.shelfno,self.bookcode)