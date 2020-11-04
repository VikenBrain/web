import time 
import pymysql


def get_time():
    """获取现在的时间戳"""
    time_str = time.strftime("%Y{}%m{}%d{} %X")
    return time_str.format("年", "月", "日")
    

def get_conn():
    """建立连接"""
    conn = pymysql.connect(host="rm-wz9yafq135l102j56mo.mysql.rds.aliyuncs.com", user="viken", password="viken2021", db="data", charset="utf8")
    # 创建游标
    consor = conn.cursor()
    return conn, consor


def close_conn(conn, cursor):
    """关闭连接"""
    if cursor:
        cursor.close()
    if conn:
        conn.close()

def query(sql, *args):
    """查询数据库"""
    conn, cursor = get_conn()
    cursor.execute(sql, args)
    res = cursor.fetchall()
    close_conn(conn, cursor)
    return res

def test():
    sql = 'select * from details'
    res = query(sql)
    return res[1]

def get_c1_data():
    sql = "select sum(confirm),(select suspect from history order by ds desc limit 1),sum(heal),sum(dead) from details where update_time=(select update_time from details order by update_time desc limit 1) "
    res = query(sql)
    return res[0]

def get_c2_data():
    sql = "select province,sum(confirm) from details " \
          "where update_time=(select update_time from details " \
          "order by update_time desc limit 1) " \
          "group by province"
    res = query(sql)
    return res

def get_l1_data():
    sql = "select ds,confirm,suspect,heal,dead from history"
    res = query(sql)
    return res

def get_l2_data():
    sql = "select ds,confirm_add,suspect_add from history"
    res = query(sql)
    return res

def get_r1_data():
    sql = 'select city,confirm from ' \
          '(select city,confirm from details ' \
          'where update_time=(select update_time from details order by update_time desc limit 1) ' \
          'and province not in ("湖北","北京","上海","天津","重庆") ' \
          'union all ' \
          'select province as city,sum(confirm) as confirm from details ' \
          'where update_time=(select update_time from details order by update_time desc limit 1) ' \
          'and province in ("北京","上海","天津","重庆") group by province) as a ' \
          'order by confirm desc limit 5'
    res = query(sql)
    return res

def get_r2_data():
    sql = "select content from hotsearch order by id desc limit 20"
    res = query(sql)
    return res


if __name__ == "__main__":
    print(get_time())
    # print(get_c2_data())


