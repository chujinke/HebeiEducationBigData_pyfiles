#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import jieba
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from scipy.misc import imread
from PIL import Image
import numpy as np
from os import path
import pymysql
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from wordcloud import WordCloud,ImageColorGenerator

def data(name,tihao):
    # 打开数据库连接

    db = pymysql.connect(host='10.187.1.157',user= 'root', passwd='Wlzx@12345678',db='new_db',charset='utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 查询语句
    sql = "SELECT "+tihao+",学校名称 FROM "+name+" WHERE ("+tihao+"!='无')and("+tihao+"!='(空)')"
    txt = ""
    name1 = ""
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            qishisan = row[0]
            # 打印结果
            txt += qishisan
        name1 = results[1][1]
    except:
        print("Error: unable to fetch data")

    # 关闭数据库连接
    db.close()
    if tihao=="qishisan":
        th = "七十三"
    else:th = "七十四"
    return txt ,name1 , th

def get_words(txt,name,th):
    # 打开数据库连接
    conn = pymysql.connect(host='localhost',user= 'root', passwd='root',db='xueqing' ,charset='utf8')

    # 使用cursor()方法获取操作游标
    #词权重
    list1 = []
    zidian = {}
    seg_list1 = jieba.cut_for_search(txt)
    list1.append(" ".join(seg_list1))
    vectorizer = CountVectorizer()  # 该类会将文本中的词语转换为词频矩阵，矩阵元素a[i][j] 表示j词在i类文本下的词频
    transformer = TfidfTransformer()  # 该类会统计每个词语的tf-idf权值
    tfidf = transformer.fit_transform(
        vectorizer.fit_transform(list1))  # 第一个fit_transform是计算tf-idf，第二个fit_transform是将文本转为词频矩阵
    word = vectorizer.get_feature_names()  # 获取词袋模型中的所有词语
    weight = tfidf.toarray()  # 将tf-idf矩阵抽取出来，元素a[i][j]表示j词在i类文本中的tf-idf权重
    for i in range(len(weight)):  # 打印每类文本的tf-idf词语权重，第一个for遍历所有文本，第二个for便利某一类文本下的词语权重
        print(u"-------这里输出文本的词语tf-idf权重------")
        for j in range(len(word)):
            zidian[word[j]] = weight[i][j]
    #词频
    seg_list = jieba.cut(txt)
    c = Counter()
    stopwords = [line.strip() for line in open("stopdata.txt", 'r', encoding='utf-8').readlines()]
    del stopwords[0]
    for x in seg_list:
        if x not in stopwords:
            if len(x) > 1 and x != '\r\n':
                c[x] += 1
    print(name+'常用词频度统计结果')
    ths = ""
    if th =="七十四":
        ths = "qishisi"
    else:ths = "qishisan"
    indata = []
    for (k, v) in c.most_common(100):
        if k in zidian:
            indata1 = (name, "2017年"+name+"问卷数据第"+th+"题云图.png", k, str(v), str(round(zidian[k],5)),'2017')
            indata.append(indata1)
        else:
            indata1 = (name, "2017年" + name + "问卷数据第" + th + "题云图.png", k, str(v),"null", '2017')
            indata.append(indata1)
    print(indata)
    sql = "INSERT INTO `2017wordfrequency`(`Universityname`, `"+ths+"img`, `Wordfrequency`, `count`, `Wordweights`, `years`) VALUES (%s,%s,%s,%s,%s,%s)"
    print(sql)
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
    list = []
    for (k, v) in c.most_common(60):
            list.append(k)
    wl = " ".join(list)
    d = path.dirname(__file__)
    alice_coloring = np.array(Image.open(path.join(d, r"D:\pro\img\abc.jpg")))
    wc = WordCloud(background_color="#263661",  # 设置背景颜色
                   mask = alice_coloring,  #设置背景图片
                   max_words=2000,  # 设置最大显示的字数
                   # width = 800,
                   # height = 700,
                   # stopwords = "", #设置停用词
                   font_path=r"C:\Windows\Fonts\msyhbd.ttc",
                   # 设置中文字体，使得词云可以显示（词云默认字体是“DroidSansMono.ttf字体库”，不支持中文）
                   max_font_size=100,  # 设置字体最大值
                   random_state=30,  # 设置有多少种随机生成状态，即有多少种配色方案
                   )
    myword = wc.generate(wl)  # 生成词云
    bg = np.array(Image.open(r"D:\pro\img\bg.png"))
    image_colors = ImageColorGenerator(bg)
    plt.imshow(myword.recolor(color_func=image_colors))
    wc.to_file(r"D:\pro\img\2017年"+name+"问卷数据第"+th+"题云图.png")

    # # 展示词云图
    # plt.imshow(myword)
    # plt.axis("off")
    # plt.show()
def run():
    xuexiaoname = []
    try:
        conn = pymysql.connect(host='10.187.1.157',user= 'root', passwd='Wlzx@12345678',db='new_db',charset='utf8')
        cur = conn.cursor()
        cur.execute('SHOW TABLES')
        #print(cur.fetchall()[len(cur.fetchall())-4][0])
        result = cur.fetchall()
        for i in range(47):
            xuexiaoname.append(result[i][0])
        cur.close()
        conn.close()
    except :
        print("Error: unable to fetch data")
    tihao = ["qishisan","qishisi"]
    for i in range(len(xuexiaoname)):
        for j in range(len(tihao)):
            txt = data(xuexiaoname[i], tihao[j])
            get_words(txt[0], txt[1], txt[2])
if __name__ == '__main__':
    run()