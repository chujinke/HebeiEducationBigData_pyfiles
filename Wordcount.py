#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import jieba
from collections import Counter
import xlrd
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from scipy.misc import imread
from PIL import Image
import numpy as np
from os import path
import pymysql
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer

def data(name):
    # 打开数据库连接

    db = pymysql.connect(host='10.187.1.157',user= 'root', passwd='****',db='new_db',charset='utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 查询语句
    sql = "SELECT qishier,qishisan,qishisi,学校名称,id FROM "+name
    datalist = []
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            qishier = row[0]
            name = row[3]
            id1 = row[4]
            if row[1] =='无'or row[1] =='(空)':
                qishisan = 0
            else:qishisan = len(row[1])
            if row[2] =='无'or row[2] =='(空)':
                qishisi = 0
            else:qishisi = len(row[2])
            # 打印结果
            datalist1 = (name,qishier,qishisan,qishisi,str(id1))
            datalist.append(datalist1)
    except:
        print("Error: unable to fetch data")

    # 关闭数据库连接
    db.close()
    return datalist
def run():
    xuexiaoname = []
    try:
        conn = pymysql.connect(host='10.187.1.157',user= 'root', passwd='***',db='new_db',charset='utf8')
        cur = conn.cursor()
        cur.execute('SHOW TABLES')
        result = cur.fetchall()
        for i in range(47):
            xuexiaoname.append(result[i][0])
        cur.close()
        conn.close()
    except :
        print("Error: unable to fetch data")
    conn = pymysql.connect(host='localhost', user='root', passwd='***', db='xueqing', charset='utf8')
    for i in range(len(xuexiaoname)):
        datalist = data(xuexiaoname[i])
        print(datalist)
        sql = "INSERT INTO `2017wordcount`(`Universityname`, `qishier`, `qishisan`, `qishisi`,`id`) VALUES (%s,%s,%s,%s,%s)"
        print(sql)
        cur = conn.cursor()
        try:
            reCount = cur.executemany(sql, datalist)
            conn.commit()
            print(reCount)

        except:
            conn.rollback()
            print("发生错误，DB回滚")
if __name__ == '__main__':
    run()
