## 一、引入jar包

```
# -*- coding: utf-8 -*-
import findspark

findspark.init()

import sys
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder. \
    appName("label_recmd_pn"). \
    config("spark.sql.shuffle.partitions", 5). \
    config("spark.default.parallelism", 5). \
    config("spark.jars","spark-streaming-kafka-0-8-assembly_2.11-2.4.8.jar").\
    enableHiveSupport(). \
    getOrCreate()


from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":

    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-0-8-assembly_2.11-2.4.8.jar pyspark-shell'
    sc = spark.sparkContext
    sc.setLogLevel("WARN") # 减少shell打印日志
    ssc = StreamingContext(sc, 5) # 5秒的计算窗口  the time interval (in seconds) at which streaming data will be divided into batches.
    brokers="""54.177.152.253:9092,13.52.56.28:9092,18.144.153.2:9092"""
    topic = "recmd_online"
    # 使用streaming使用直连模式消费kafka
    # 参数：ssc StreamingContext
    #        topic 名称
    #       kafka节点
    kafka_streaming_rdd = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines_rdd = kafka_streaming_rdd.map(lambda x: x[1])
    counts = lines_rdd.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    # 将workcount结果打印到当前shell
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()

```

通过 config配置之后，就可以使用`python3 xxx.py执行`

## 二、offset相关

从 KafkaUtils.createDirectStream直接建立InputStream流，默认是从最新的偏移量消费

driver端通过使用*transform*获取到offset信息，然后在输出操作*foreachrdd*里面完成offset的提交操作。

```typescript
def store_offset_ranges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd


def save_offset_ranges(rdd):
    root_path = os.path.dirname(os.path.realpath(__file__))
    record_path = os.path.join(root_path, "offset.txt")
    f = open(record_path, "w")
    for o in offsetRanges:
        data = {"topic": o.topic, "partition": o.partition, "fromOffset": o.fromOffset, "untilOffset": o.untilOffset}
        f.write(json.dumps(data))
        f.write("\r\n")
    f.close()


def deal_data(rdd):

    data = rdd.collect()
    print(data[:1])
    for d in data:
        # do something
        pass

def save_by_spark_streaming():
    root_path = os.path.dirname(os.path.realpath(__file__))
    record_path = os.path.join(root_path, "offset.txt")
    from_offsets = {}
    # 获取已有的offset，没有记录文件时则用默认值即最大值
    if os.path.exists(record_path):
        f = open(record_path, "r")
        res = f.readlines()
        f.close()
        for item in res:
            item = item.strip()
            if item:
                offset_data = eval(item)
                topic_partion = TopicAndPartition(offset_data["topic"], offset_data["partition"])
                from_offsets[topic_partion] = offset_data["untilOffset"]  # 注意设置起始offset时的方法
        print ("start from offsets: %s" % from_offsets)

    sc = spark.sparkContext
    ssc = StreamingContext(sc, int(timer))

    kafkaParams = {
        "metadata.broker.list": broker_list,
        "group.id": "recmd_online"
    }

    kvs = KafkaUtils.createDirectStream(ssc=ssc, topics=[topic_name], fromOffsets=from_offsets,
                                        kafkaParams=kafkaParams)

    kvs.foreachRDD(lambda rec: deal_data(rec))
    kvs.transform(store_offset_ranges).foreachRDD(save_offset_ranges)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
```

### 1、offsetRanges()

DStream.transform对DStream中的每个rdd进行处理。`rdd.offsetRanges()`返回该topic下所有partition的信息。比如有3个paitition，则其返回值为一个列表。列表中的每个元素可以解析成一个字典：

```
[{"topic": "recmd_online", "partition": 1, "fromOffset": 236661, "untilOffset": 236661}
{"topic": "recmd_online", "partition": 0, "fromOffset": 231132, "untilOffset": 231132}
{"topic": "recmd_online", "partition": 2, "fromOffset": 233506, "untilOffset": 233506}]
```

指定了topic,分区,起始偏移量，接收偏移量。

### 2、createDirectStream()

创建DStream
fromOffsets为空时，之前存放在Kafka的信息不处理。要指定从哪个offset开始读取，需要传入fromOffsets参数

该参数为一个字典：

```
{
	TopicAndPartition("recmd_online", 1):236661,
	TopicAndPartition("recmd_online", 0):231132,
	TopicAndPartition("recmd_online", 2):233506	
}
```

### 3、保存offset

为了重启任务之后，从最近一次的消费位置继续消费，需要将offset保存到本地或任意数据库中

```
 f = open(record_path, "w")
    for o in offsetRanges:
        data = {"topic": o.topic, "partition": o.partition, "fromOffset": o.fromOffset, "untilOffset": o.untilOffset}
        f.write(json.dumps(data))
        f.write("\r\n")
```

## 三、去重

我现在是这么做的，适用于用户量不大的情况，当前DAU为100W

1、spark streaming接收前端pingback日志，提取<用户ID，查看的item列表>数据；

2、使用redis cluster的map结构，以用户ID为key，以item列表的json为value，更新；

3、下次查询的时候，取出这个item列表，过滤掉；



其中spark streaming的处理时间为10秒钟一个窗口；

另外，每天会清空这个用户3天前的观看历史，也就是3天前的item会仍然再次推送；

我们是类似电商，所以没买过的会过一段时间再推；



作者：裴帅帅
链接：https://www.zhihu.com/question/345071035/answer/826045583
来源：知乎
著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。



## 四、参数解析

### 1、SparkSession

新的spark程序入口

### 2、SparkContext

```
 sc = spark.sparkContext
```

### 3、StreamingContext

```
ssc = StreamingContext(sc, int(timer))
```

:param sparkContext: :class:`SparkContext` object.
:param batchDuration: the time interval (in seconds) at which streaming
                              data will be divided into batches

### 4、kafka连接

```
 kafkaParams = {
        "metadata.broker.list": broker_list,
        "group.id": "recmd_online"
    }

kvs = KafkaUtils.createDirectStream(ssc=ssc, topics=[topic_name], fromOffsets=from_offsets,
                                        kafkaParams=kafkaParams)
```

1、ssc，

2、topics：kafka topic组成的列表

3、fromOffsets：确定每个topic每个分区的起始偏移量。一个字典，key为TopicAndPartition对象，value为偏移量

```
{
	TopicAndPartition("recmd_online", 1):236661,
	TopicAndPartition("recmd_online", 0):231132,
	TopicAndPartition("recmd_online", 2):233506	
}
```

4、kafkaParams：kafka连接参数

