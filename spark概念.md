![](image\yarn后台.png)



### 一、SparkSession配置

```
# 单机配置
spark = SparkSession.builder. \
    appName("label_recmd_pn"). \
    config("spark.sql.shuffle.partitions", 5). \
    config("spark.default.parallelism", 5). \
    config("hive.warehouse.subdir.inherit.perms", "false"). \
    config('spark.driver.memory', '5g').\
    enableHiveSupport(). \
    getOrCreate()

```

#### 1、appName

设置应用的名字，在管理页面可以看到Name。

#### 2、spark.sql.shuffle.partitions

默认值=200

spark.default.parallelism只有在处理RDD时才会起作用，对Spark SQL的无效。
spark.sql.shuffle.partitions则是对Spark SQL专用的设置

```
spark.sql(sql_1).rdd # sql查出来的数据得到的RDD的分区数=spark.sql.shuffle.partitions
```



#### 3、spark.default.parallelism

| Property Name                | Default                                                      | Meaning                                                      |
| ---------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| spark.sql.shuffle.partitions | 200                                                          | Configures the number of partitions to use when shuffling data for joins or aggregations. |
| spark.default.parallelism    | For distributed shuffle operations like reduceByKey and join, the largest number of partitions in a parent RDD.For operations like parallelize with no parent RDDs, it depends on the cluster manager: - Local mode: number of cores on the local machine - Mesos fine grained mode: 8 - Others: total number of cores on all executor nodes or 2, whichever is larger | Default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize when not set by user. |

开始为2个分区，经过groupByKey之后变成了5个分区

```
sc = spark.sparkContext
list1 = dict(enumerate(range(1,200)))
list2 = list()
for k,v in list1.items():
    if k<=100:
       list2.append((k,v))
    else:
        k= k-100
        list2.append((k,v))
rdd = sc.parallelize(list2,2)
print(rdd.getNumPartitions())
rdd = rdd.groupByKey().mapValues(list)
print(rdd.getNumPartitions())
```

#### 4、hive.warehouse.subdir.inherit.perms

```
  config("hive.warehouse.subdir.inherit.perms", "false")
```

如果设置inherit.perms 为true，则 子表的权限设置继承自warehouse 目录的设置。 

#### 5、enableHiveSupport(）

使得可以查询hive

#### 6、spark.driver.cores

```
config("spark.driver.cores",1) # 默认=1
```

Number of cores to use for the driver process, only in cluster mode. 

#### 7、spark.driver.maxResultSize

```
config("spark.driver.maxResultSize",'3g') # 默认='1g'
```

Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.

如果使用默认值，同时代码有很多collect()，会有以下错误。就需要增大该值。

```
Job aborted due to stage failure: Total size of serialized results of 3979 tasks (1024.2 MB) is bigger than spark.driver.maxResultSize (1024.0 MB)
```

#### 8、spark.driver.memory

```
config("spark.driver.memory","5g") # 默认="1g"
```

Amount of memory to use for the driver process, i.e. where SparkContext is initialized, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. `512m`, `2g`).
*Note:* In client mode, this config must not be set through the `SparkConf` directly in your application, because the driver JVM has already started at that point. Instead, please set this through the `--driver-memory` command line option or in your default properties file.

driver memory并不是master分配了多少内存，而是管理多少内存。换言之就是为当前应用分配了多少内存。

在client模式下driver的堆内存，不要通过SparkConf设置，要用--driver-memory命令替换，或者在默认的配置文件里配置。

#### 9、spark.driver.memoryOverhead

```
config("spark.driver.memoryOverhead","384M") # driverMemory * 0.10, with minimum of 384
```

Amount of non-heap memory to be allocated per driver process in cluster mode, in MiB unless otherwise specified. This is memory that accounts for things like VM overheads, interned strings, other native overheads, etc. This tends to grow with the container size (typically 6-10%). This option is currently supported on YARN, Mesos and Kubernetes. *Note:* Non-heap memory includes off-heap memory (when `spark.memory.offHeap.enabled=true`) and memory used by other driver processes (e.g. python process that goes with a PySpark driver) and memory used by other non-driver processes running in the same container. The maximum memory size of container to running driver is determined by the sum of `spark.driver.memoryOverhead` and `spark.driver.memory`.

默认值是max(DriverMemory*0.1,384m)。在YARN的cluster模式下，driver端申请的off-heap内存的总量，通常是driver堆内存的6%-10%。

