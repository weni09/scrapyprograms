import pymysql

class mysqlOperator:
    # 构造函数
    def __init__(self,host,port,user,pwd,db_name):
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db_name = db_name
        self.db = None
        self.cur = None
    def connect_db(self):
        """
            连接数据库
        """
        try:
            self.db = pymysql.connect(host=self.host,port=self.port,user=self.user,password=self.pwd,database=self.db_name,charset='utf8mb4')
        except pymysql.Error as e:
            print("pymysql Error:%s" % e)
            return False
        self.cur = self.db.cursor()
        return True
    def close(self):
        """
        关闭数据库
        """
        if self.db and self.cur:
            self.cur.close()
            self.db.close()
        return True
    def execute(self,sql, params=None):
        """
        执行sql语句
        """
        self.connect_db()
        try:
            if self.db and self.cur:
                # 正常逻辑,执行sql，提交操作
                self.cur.execute(sql, params)
                self.db.commit()
        except:
            print('execute failed, sql = ', sql)
            self.db.rollback()
            #self.close()
            return False
        return True

    def executemany(self,sql,params=None):
        """
        执行sql语句,多值
        """
        self.connect_db()
        try:
            if self.db and self.cur:
                # 正常逻辑,执行sql，提交操作
                self.cur.executemany(sql,params)
                self.db.commit()
        except:
            print('execute failed, sql = ', sql)
            self.db.rollback()
            # self.close()
            return False
        return True
    def fetch_all(self, sql, params=None):
        """
        查询数据
        """
        self.execute(sql, params)
        return self.cur.fetchall()



#mysqlcon=mysqlOperator(host='192.168.200.129',port=3306,user='root',pwd='123456',db_name='wenwei')
#print(mysqlcon.fetch_all("select S_ORDER_NO,ORDER_NO,GOODS_CODE,DEMAND_ID,DELIVERY_DATE from ordertms.ots_order_item where S_ORDER_NO in (81118469,81118470,81118471,81118472,81118473);"))
#mysqlcon.close()
#mysqlcon.execute('''drop table wenwei.test_py''')
#mysqlcon.execute("create table wenwei.test_py(id int unsigned)")
#b=mysqlcon.execute("create table test_py(id int);")
#mysqlcon.execute("insert into wenwei.test_py values (888);")
#mysqlcon.execute("delete from wenwei.test_py;")