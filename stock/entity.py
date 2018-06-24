
class StockPrice:

    def __init__(self, id, code, txnDate, open,  high, low, close, volume):
        self.id = id
        self.code = code
        self.txnDate = txnDate
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def createInsertSql(self):
        sql = "insert into STOCKDATA (ID, CODE, TXNDATE, OPEN, HIGH, LOW, CLOSE, VOLUME) " \
              "values ('%s', '%s', %s, %s, %s, %s, %s, %s)" % \
              (self.id, self.code, self.txnDate, self.open, self.high, self.low, self.close, self.volume)
        return sql


class StockInfo:
    def __init__(self, code, name, listeddate=None):
        self.code = code
        self.listeddate = listeddate
        self.name = name

    def createInsertSql(self):
        return "insert into STOCKINFO (CODE, NAME)  values ('"+self.code+"', '"+self.name+"')"