import requests
import time
from lxml import etree
import pandas as pd
from sqlalchemy import create_engine


class KYLC():

    def __init__(self):

        self.url = 'https://www.kylc.com/stats/global/yearly/g_gdp/{}.html'
        self.headers = {
            'Accept - Encoding': 'gzip, deflate',
            'Accept - Language': 'zh - CN, zh;q = 0.9, en;q = 0.8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'}
        self.time = [i for i in range(1960,2020)]



    def get_raw_data(self):
        tables = pd.DataFrame()
        """GDP表的编制是从1960年开始的"""
        for i in self.time:
            url = self.url.format(i)
            time.sleep(1)
            resp = requests.get(url, headers= self.headers).text
            tree = etree.HTML(resp)
            code_list = tree.xpath('//div[@class="container"]//tbody/tr')
            for code in code_list:
                year = [i]
                country = code.xpath('./td[2]/text()')
                continent = code.xpath('./td[3]/text()')
                gdp1 = code.xpath('./td[4]/text()')
                gdp_per = code.xpath('./td[5]/text()')
                table = pd.DataFrame([year, country, continent, gdp1, gdp_per]).T
                table.columns = ['year', 'country', 'continent', 'gdp1', 'gdp_per']
                tables = pd.concat([tables, table])

        return tables

    def get_process_data(self, grd):
        # 1. 删除缺失值, 缺失项在2-3
        df = grd.dropna(axis=0, thresh=3, inplace=False)
        df1 = pd.DataFrame(df['gdp1'].map(lambda x: x.split(" ")[1]))
        df2 = df1.applymap(lambda x: x.replace(')', ''))
        df3 = df2.applymap(lambda x: x.replace('(', ''))
        df4 = df3.applymap(lambda x: x.replace(',', ''), )
        df4.columns = ['gdp']
        df5 = pd.concat([df, df4], axis=1, sort=False)
        df5.drop(['gdp1'], axis=1, inplace=True)
        # 将continent里面确实的世界和欧盟填充为1
        df6 = df5.fillna(value= 1)
        # 查看还有数据的缺失情况
        # print(df5.isnull().sum(axis=0))
        return df6

    def into_mysql(self, gpd):
        # MySQL的用户：root, 密码:147369, 端口：3306,数据库：test
        db_info = {
            'user': 'viken',
            'password': 'viken2021',
            'host': 'rm-wz9yafq135l102j56mo.mysql.rds.aliyuncs.com',
            'port': 3306,
            'database': 'data',
        }
        conn = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % db_info, encoding='utf-8')
        # 将新建的DataFrame储存为MySQL中的数据表，储存index列
        # if_exists:
        # 1.fail:如果表存在，啥也不做
        # 2.replace:如果表存在，删了表，再建立一个新表，把数据插入
        # 3.append:如果表存在，把数据插入，如果表不存在创建一个表！！
        # gpd.to_sql('GDP', conn, index=False, if_exists='replace')
        pd.io.sql.to_sql(gpd, 'year_gdp_data', conn,  index=True, if_exists='append')
        print('数据已经完毕!')


    def run(self):
        grd = self.get_raw_data()
        gpd = self.get_process_data(grd)
        im = self.into_mysql(gpd)


if __name__ == '__main__':
    KYLC().run()
