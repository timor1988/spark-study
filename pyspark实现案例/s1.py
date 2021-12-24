# -*- coding: utf-8 -*-

import findspark

findspark.init()
from pyspark import SparkContext
from pyspark.sql import Row

sc = SparkContext("local", "First App")

"""
//用户访问动作表
case class UserVisitAction(
 date: String,//用户点击行为的日期 0
 user_id: Long,//用户的 ID 1
 session_id: String,//Session 的 ID 2
 page_id: Long,//某个页面的 ID 3
 action_time: String,//动作的时间点 4
 search_keyword: String,//用户搜索的关键词 5
 click_category_id: Long,//某一个商品品类的 ID 6
 click_product_id: Long,//某一个商品的 ID 7
 order_category_ids: String,//一次订单中所有品类的 ID 集合 8
 order_product_ids: String,//一次订单中所有商品的 ID 集合 9
 pay_category_ids: String,//一次支付中所有品类的 ID 集合 10
 pay_product_ids: String,//一次支付中所有商品的 ID 集合 11
 city_id: Long, //城市 id 12
)
"""


class Utils:

    @staticmethod
    def preprocess(x):
        """
        数据预处理。字符串分割之后取有用的字段
        :param x:
        :return:
        """
        temp = x.split(",")
        return temp

    @staticmethod
    def nedd1(x):

        item_id = x[6]
        # 如果是点击
        if item_id != "-1":
            return [(item_id, (1, 0, 0))]
        elif x[8]:  # 如果是下单,该值是品类id的集合。
            ids = x[8].split("-")
            ids = [(id_, (0, 1, 0)) for id_ in ids]
            return ids
        elif x[10]:
            ids = x[10].split("-")
            ids = [(id_, (0, 0, 1)) for id_ in ids]
            return ids
        else:
            return []


class Main:

    @staticmethod
    def start():
        rdd = sc.textFile("user_visit_action.csv")
        rdd = rdd.map(Utils.preprocess)
        rdd.cache()
        # Main.need1(rdd)
        #Main.need2(rdd)
        Main.need3(rdd)
        rdd.unpersist()
        sc.stop()

    @staticmethod
    def need1(rdd):
        """
        1.每个品类的点击、下单、支付的量来统计热门品类。三个不同的数据源合并在一起
        2. 将数据转换结构
        点击的场合 : ( 品类ID，( 1, 0, 0 ) )
        下单的场合 : ( 品类ID，( 0, 1, 0 ) )
        支付的场合 : ( 品类ID，( 0, 0, 1 ) )
        :param rdd:
        :return:
        """
        rdd = rdd.flatMap(Utils.nedd1)
        rdd = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
        rdd = rdd.sortBy(lambda x: (x[1][0], x[1][1], x[1][2]), ascending=False)  # 默认为升序
        # rdd.foreach(print)
        return rdd

    @staticmethod
    def need2(rdd):
        """
        Top10 热门品类中每个品类的 Top10 活跃 Session 统计
        在需求一的基础上，增加每个品类用户 session 的点击统计
        :param rdd:
        :return:
        """
        # 1、top10品类
        top10 = Main.need1(rdd).map(lambda x: x[0]).take(10)

        # 2、过滤原始数据,保留点击和前10品类ID
        def filter_item(x):
            """
            过滤session_id
            :param x:
            :return:
            """
            item_id = x[6]
            if item_id != "-1":  # 说明是点击行为
                if item_id in top10:
                    return True
            return False

        rdd = rdd.filter(lambda x: filter_item(x))
        # 3、根据品类ID和sessionid进行点击量的统计
        rdd = rdd.map(lambda x: ((x[6], x[2]), 1)).reduceByKey(lambda x, y: x + y)
        # 4、将统计的结果进行结构的转换
        rdd = rdd.map(lambda x: (x[0][0], (x[0][1], x[1])))  # (品类id,sessionid),数量 => 品类id,(session_id，数量)
        # 5、相同的品类进行分组
        rdd = rdd.groupByKey()

        # 6、将分组后的数据进行点击量的排序，取前10名
        def rank(items):
            """
            排序
            :param items:
            :return:
            """
            res = sorted(items, key=lambda x: x[1], reverse=True)
            return [item[0] for item in res[:10]]

        rdd = rdd.mapValues(rank)
        result = rdd.collect()
        for item in result:
            print(item)

    @staticmethod
    def need3(rdd):
        """
        计算给定跳转路径的单跳转化率
        比如计算 3-5 的单跳转化率，
        先获取符合条件的 Session 对于页面 3 的访问次数（PV） 为 A，
        然后获取符合条件的 Session 中访问了页面 3
        又紧接着访问了页面 5 的次数为 B，
        那么 B/A 就是 3-5 的页面单跳转化率。
        :param rdd:
        :return:
        """

        # 参考scala样例类，python使用pyspark的row可破
        def list2dict(x):
            return Row(
                date=x[0],
                user_id=x[1],
                session_id=x[2],
                page_id=x[3],  # 某个页面的ID
                action_time=x[4],  # 动作的时间点
                search_keyword=x[5],  # 用户搜索的关键词
                click_category_id=x[6],  # 某一个商品品类的ID
                click_product_id=x[7],  # 某一个商品的ID
                order_category_ids=x[8],  # 一次订单中所有品类的ID集合
                order_product_ids=x[9],  # 一次订单中所有商品的ID集合
                pay_category_ids=x[10],  # 一次支付中所有品类的ID集合
                pay_product_ids=x[11],  # 一次支付中所有商品的ID集合
                city_id=x[12]  # 城市id
        )

        rdd = rdd.map(lambda x: list2dict(x))

        # 1、对指定的页面连续跳转进行统计，确定统计页面
        # 1 - 2, 2 - 3, 3 - 4, 4 - 5, 5 - 6, 6 - 7
        ids = ["1","2","3","4","5","6","7"]
        ok_flow_ids = list(zip(ids,ids[1:]))
        # 2、计算分母
        fenmu = rdd.filter(lambda x:x["page_id"] in ids).map(lambda x: (x["page_id"],1)).\
            reduceByKey(lambda x,y: x+y).collectAsMap()
        print(fenmu)
        # 3、计算分子
        # session进行分组。分组后，根据访问时间进行排序（升序）
        def my_rank(items):
            """

            :param items:
            :return:  [((1,2),1),((2,3),1)...]
            """
            items = sorted(items,key=lambda x:x["action_time"])
            flow_ids = [item["page_id"] for item in items]
            page_follow_ids = list(zip(flow_ids,flow_ids[1:])) # 拉链
            # 将不合法的页面跳转进行过滤
            page_follow_ids = [(item,1) for item in page_follow_ids if item in ok_flow_ids]
            return page_follow_ids

        session_rdd = rdd.groupBy(lambda x: x["session_id"]).mapValues(my_rank)
        session_rdd = session_rdd.flatMap(lambda x:x[1])# 去掉用户，值保留页面跳转信息
        session_rdd = session_rdd.reduceByKey(lambda x,y:x+y)

        # 4、分子/分母，计算单跳转化率
        def calculate(x):

            fenzi = x[1]  # 1. 分子数值
            fenmu_ = fenmu.get(x[0][0])
            result = fenzi/fenmu_
            return ((x[0],x[1]),result)

        session_rdd = session_rdd.map(lambda x:calculate(x))
        session_rdd.foreach(print)



if __name__ == '__main__':
    #Main.start()
    rdd = sc.parallelize([1,2,3,4,5,6,0])
    rdd = rdd.map(lambda x:x/1)
    print(rdd.collect())
