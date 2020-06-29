# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pandas as pd
import re
from pachong1.dbtools import mysqloperator
class Pachong1Pipeline:
    def __init__(self):
        self.writenum = 0
    def process_item(self, item, spider):
        #print(item['id'],item['name'])
        #itemdf['parentId'] = list(item.get('parentId'))
        #itemdf['level'] = list(item.get('level'))
        #idList,nameList=[],[]
        idList=list(item['id'])
        nameList=list(item['name'])

        idList_new = list(map(lambda x:re.match(r'([0-9]*[1-9]){0,9}',x).group(0),idList))
        #print(idList_new)
        for i in range(len(idList_new)):
            while 0 < len(idList_new[i]) < 6 and len(idList_new[i]) % 2 != 0:
                idList_new[i] = str(idList_new[i]) + '0'
            while 6 < len(idList_new[i]) < 9:
                idList_new[i] = str(idList_new[i]) + '0'
        #print(idList_new)
        parentIdList,levelList= [],[]
        for j in idList_new:
            if len(j) < 9:
                parentIdLen = len(j) - 2
                level = int(len(j) / 2)
            else:
                parentIdLen = len(j) - 3
                level = int((len(j) - 6) / 3) + 3
            parentIdList.append(j[:parentIdLen])
            levelList.append(level)
        #//转化dataframe,输出到csv
        item_df = pd.DataFrame([idList_new,nameList,parentIdList,levelList])
        item_df = item_df.T
        item_df.rename(columns={0:'id',1:'name',2:'parentId',3:'level'},inplace=True)
        #print(item_df)
        #//初始化写入文件时，不写入头，模式为追加写入。
        header,mode=False,'a'
        #//初次写入时，写入头，模式为覆盖写入。
        if self.writenum == 0:
            header,mode = True,'w'
        item_df.to_csv('./addivtest.csv',mode=mode,encoding='utf-8',header=header,index=False)

        #//转化为元组对列表，插入mysql
        output = []
        output.extend(zip(idList_new, nameList, parentIdList, levelList))
        mysqlConn = mysqloperator.mysqlOperator(host='192.168.200.129', port=3306, user='root', pwd='123456',db_name='wenwei')
        #//初次入库，清空表
        if self.writenum == 0:
            mysqlConn.execute("truncate table ad_level_sigle;")
        #//数据入库
        i_sql = "insert into ad_level_sigle(id,name,parentid,level) values(%s,%s,%s,%s);"
        mysqlConn.executemany(i_sql, output)
        mysqlConn.close()
        
        self.writenum += 1
        return self.writenum