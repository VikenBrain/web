#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, render_template
from data import SourceData

# __name__是系统变量，该变量指的是本py文件的文件名
app = Flask(__name__)
source = SourceData()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/line')
def line():
    data = source.line
    xAxis = data.pop('legend')
    return render_template('line.html', title='折线图示例', data=data, legend=list(data.keys()), xAxis=xAxis)

@app.route('/bar')
def bar():
    data = source.bar
    xAxis = data.pop('legend')
    return render_template('bar.html', title='柱形图示例', data=data, legend=list(data.keys()), xAxis=xAxis)

@app.route('/pie')
def pie():
    data = source.pie
    return render_template('pie.html', title='饼图示例', data=data, legend=[i.get('name') for i in data])

@app.route('/china')
def china():
    data = source.china
    return render_template('china.html', title='地图示例', data=data)


@app.route('/wordcloud')
def wordcloud():
    data = source.wordcloud
    return render_template('wordcloud.html', title='词云示例', data=data)







if __name__ == '__main__':
    app.run(host= '127.0.0.1', debug= True)







