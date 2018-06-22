# coding=utf-8
from collections import Counter
import jieba
import numpy as np
import math
from scipy.misc import imread
from PIL import Image
import numpy as np
from os import path
import pymysql
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer

def data(name,tihao):
    # 打开数据库连接

    db = pymysql.connect(host='10.187.1.157',user= 'root', passwd='***',db='***',charset='utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 查询语句
    sql = "SELECT "+tihao+",学校名称,id FROM "+name+" WHERE ("+tihao+"!='无')and("+tihao+"!='(空)')"
    txt = ""
    name1 = ""
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            qishisan = row[0]
            id = row[2]
            # 打印结果
            txt += str(id)+"&&"+qishisan+"\n"
        name1 = results[1][1]
    except:
        print("Error: unable to fetch data")

    # 关闭数据库连接
    db.close()
    if tihao=="qishisan":
        th = "七十三"
    else:th = "七十四"
    return txt ,name1 , th

# 向量的模
def mo(list):
    x = 0
    for i in list:
        x = x + i**2
    return  math.sqrt(x)
if __name__ == "__main__":

    #########################################################################
    #                           第一步 计算TFIDF
    # 将文本中的词语转换为词频矩阵
    xuexiaoname = []
    try:
        conn = pymysql.connect(host='10.187.1.157', user='root', passwd='****', db='new_db', charset='utf8')
        cur = conn.cursor()
        cur.execute('SHOW TABLES')
        # print(cur.fetchall()[len(cur.fetchall())-4][0])
        result = cur.fetchall()
        for i in range(47):
            xuexiaoname.append(result[i][0])
        cur.close()
        conn.close()
    except:
        print("Error: unable to fetch data")
    tihao = ["qishisan", "qishisi"]
    xuexiaoname.remove("2017hebeidaxuegongshangxueyuan")
    for ii in range(len(xuexiaoname)):
        for jj in range(len(tihao)):
            shili = data(xuexiaoname[ii], tihao[jj])
            corpus = []
            contents = []
            txt = shili[0]
            seg_list = jieba.cut(txt)
            c = Counter()
            stopwords = [line.strip() for line in open("stopwod2.txt", 'r', encoding='utf-8').readlines()]
            del stopwords[0]
            for x in seg_list:
                if x not in stopwords:
                    if len(x) > 1 and x != '\r\n':
                        c[x] += 1
            print('常用词频度统计结果')
            for (k, v) in c.most_common(40):
                print('%s %s  %d' % (k, '*' * int(v / 3), v))
            list = []
            for (k, v) in c.most_common(40):
                list.append(k)
            print(list)
            cen = txt.split("\n")
            cen1 = []
            cen2 = []
            sy = 0
            for i in cen:
                sy = sy + 1
                if i != '(空)' and i != "无" and i != "":
                    cen1.append(str(sy) + " " + i)
                    cen2.append(i)
            textxl = []
            for i in cen2: # 词向量化
                textxl1 = []
                for j in list:
                    if j in i:
                        textxl1.append(1)
                    else:
                        textxl1.append(0)
                textxl.append(textxl1)
            Y = np.array(textxl)
            textxl20 = []
            for i in range(len(textxl)):# 根据向量化的词矩阵，计算每条评论的词相似度
                textxl21 = []
                for j in range(i,len(textxl)):
                    xlcj = np.dot(Y[i],Y[j])
                    xlmc = mo(Y[i])*mo(Y[j])
                    if xlmc !=0:
                        textxl21.append(xlcj/xlmc)
                    else:textxl21.append(0)
                textxl20.append(textxl21)
            print(textxl20[0])
            result = []
            result_item = []
            for i in range(len(textxl20)):# 将评论之间的词相似度大于0.6（可以更改）的归并为一类。
                result1 = []
                if i in result_item:
                    continue
                else:
                    for j in range(len(textxl20[i])-i):
                        if textxl20[i][j]>0.6:
                            result1.append(cen2[j+i]+"\n")
                            result_item.append((j+i))
                result.append(result1)
            print(result)
            indata = []
            num = 0
            conn = pymysql.connect(host='localhost', user='root', passwd='****', db='xueqing', charset='utf8')
            for i in range(len(result)):
                if len(result[i])>0:
                    for j in range(len(result[i])):
                        try:
                            indata1 = (shili[1],shili[2],result[i][0].split("\n")[0].split("&&",1)[1],result[i][j].split("\n")[0].split("&&",1)[1],"2017",result[i][j].split("\n")[0].split("&&",1)[0],len(result[i])-num)
                            indata.append(indata1)
                        except:
                            num = num+1
                            continue
                else:
                    continue
            sql = "INSERT INTO `2017Clustering`(`universityname`, `Questionnumber`, `category`, `content`,`years`,`id`,`count`) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            print(sql)
            print(indata)
            cur = conn.cursor()
            try:
                reCount = cur.executemany(sql, indata)
                conn.commit()
                print(reCount)
            except:
                conn.rollback()
                print("发生错误，DB回滚")

            cur.close()
            conn.close()
