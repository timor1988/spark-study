## 一、spark架构

### 1、结构图

![](image\331802-20151212204855637-422830636.png)

当运行在yarn集群上时，Yarn的ResourceMananger用来管理集群资源，集群上每个节点上的NodeManager用来管控所在节点的资源，从yarn的角度来看，每个节点看做可分配的资源池，当向ResourceManager请求资源时，它返回一些NodeManager信息，这些NodeManager将会提供execution container给你，每个execution container就是满足请求的堆大小的JVM进程，JVM进程的位置是由ResourceMananger管理的，不能自己控制，如果一个节点有64GB的内存被yarn管理（通过yarn.nodemanager.resource.memory-mb配置),当请求10个4G内存的executors时，这些executors可能运行在同一个节点上。

当在集群上执行应用时，job会被切分成stages,每个stage切分成task,每个task单独调度，可以把executor的jvm进程看做task执行池。

每个executor有 $$spark.executor.cores/spark.task.cups$$个执行槽。

实例：集群有12个节点运行Yarn的NodeManager，每个节点有64G内存和32的cpu核，每个节点可以启动2个executor，每个executor的使用26G内存，剩下的内用系统和别的服务使用，每个executor有12个cpu核用于执行task。

整个集群执行槽数：

$$12 machines* 2executors per machine* 12cores/1core=288$$

意味着集群可以同时运行288个task。

集群缓存数据的内存有：

$$0.9 spark.storage.safetyFraction * 0.6 spark.storage.memoryFraction * 12 machines * 2 executors  * 26 GB  = 336.96 GB$$



### 2、名词解释

| Term            | Meaning                                                      |
| --------------- | ------------------------------------------------------------ |
| Application     | 用户构建在 Spark 上的应用程序。由集群上的一个 *driver 进程* 和多个 *executor* 组成 |
| Application jar | 一个包含用户 Spark 应用的 Jar。有时候用户会想要去创建一个包含他们应用以及它的依赖的 “uber jar”。用户的 Jar 应该没有包括 Hadoop 或者 Spark 库，然而，它们将会在运行时被添加。 |
| Driver program  | 该进程运行应用的 main() 方法并且创建了 *SparkContext。*      |
| Cluster manager | 一个外部的用于获取集群上资源的服务。（例如，Standlone Manager，Mesos，YARN） |
| Deploy mode     | 根据 driver 程序运行的地方区别。在 “Cluster” 模式中，框架在群集内部启动 driver。在 “Client” 模式中，submitter（提交者）在 Custer 外部启动 driver。 |
| Worker node     | 在集群中可以运行应用程序代码的任何节点。                     |
| Executor        | 一个为了在 worker 节点上的应用而启动的进程，它运行 task 并且将数据保持在内存中或者硬盘存储。每个应用有它自己的 Executor。一个executor就是一个jvm进程。 |
| container       | 一个executor就是一个container                                |
| Task            | 一个将要被发送到 Executor 中的工作单元。作为exector的jvm进程中的一个线程执行 |
| Job             | 一个由多个任务组成的并行计算，并且能从 Spark action 中获取响应（例如 save，collect）; 你将在 driver 的日志中看到这个术语。 |
| Stage           | 每个 Job 被拆分成更小的被称作 *stage* (阶段）的 *task*（任务）组，*stage* 彼此之间是相互依赖的（与 *MapReduce* 中的 map/reduce *stage* 相似）。你将在 driver 的日志中看到这个术语。 |



1、broadcast原理

![](image\broadcast原理.png)



## 二、spark内存管理

### 1、堆内内存和堆外内存

作为一个JVM进程，Executor的内存管理建立在JVM内存管理之上，此外spark引入了堆外内存：不在JVM中的内存，即不属于该executor的内存。

堆内内存：由 JVM 控制，由GC（垃圾回收）进行内存回收，堆内内存的大小，由 Spark 应用程序启动时的 executor-memory 或 spark.executor.memory 参数配置，这些配置在 spark-env.sh 配置文件中。
堆外内存：不受 JVM 控制，可以自由分配
堆外内存的优点： 减少了垃圾回收的工作。
堆外内存的缺点：
堆外内存难以控制，如果内存泄漏，那么很难排查
堆外内存相对来说，不适合存储很复杂的对象。一般简单的对象或者扁平化的比较适合。

#### 2、JVM堆内内存

Executor 内运行的并发任务共享 JVM 堆内内存。JVM内部的内存分为4部分：

- Storage内存：此内存用于RDD缓存数据、广播变量等；
- Execution内存：执行shuffle时占用的内存，主要用于存放shuffle,join，sort等计算过程中的临时数据
- User内存：在这里存储自定义数据结构、udf、UDAFs等；还有RDD转换所需要的数据，RDD依赖信息
- Reserved内存：系统预留内存，存储spark内部对象，从spark 1.6起硬编码为300MB。

### 3、堆外内存

为了进一步优化内存的使用以及提高 Shuffle 时排序的效率，Spark 1.6 引入了堆外（Off-heap）内存，使之可以直接在工作节点的系统内存中开辟空间，存储经过序列化的二进制数据。

默认情况下堆外内存并不启用，可通过配置 spark.memory.offHeap.enabled 参数启用，并由 spark.memory.offHeap.size 参数设定堆外空间的大小，单位为字节。堆外内存与堆内内存的划分方式相同，所有运行中的并发任务共享存储内存和执行内存。

如果堆外内存被启用，那么 Executor 内将同时存在堆内和堆外内存，两者的使用互补影响，这个时候 Executor 中的 Execution 内存是堆内的 Execution 内存和堆外的 Execution 内存之和，同理，Storage 内存也一样。相比堆内内存，堆外内存只区分 Execution 内存和 Storage 内存。

### 4、统一内存管理

Spark 1.6 之后引入的统一内存管理机制，与静态内存管理的区别在于存储内存和执行内存共享同一块空间，可以动态占用对方的空闲区域，

#### 4.1 堆内内存

如图 所示

![](image\统一内存管理.png)

#### 4.2、堆外内存如图

![](image\1_QiEGPjaFlLgACMKhT0hboQ.png)

![](image\堆外内存.png)

#### 4.3、动态占用机制

其中最重要的优化在于动态占用机制，其规则如下：

程序提交的时候我们都会设定基本的 Execution 内存和 Storage 内存区域（通过 spark.memory.storageFraction 参数设置）；

在程序运行时，如果双方的空间都不足时，则存储到硬盘；将内存中的块存储到磁盘的策略是按照 LRU 规则进行的。若己方空间不足而对方空余时，可借用对方的空间;（存储空间不足是指不足以放下一个完整的 Block）

Execution 内存的空间被对方占用后，可让对方将占用的部分转存到硬盘，然后"归还"借用的空间，Storage 占用 Execution 内存的数据被回收后，重新计算即可恢复。

Storage 内存的空间被对方占用后，目前的实现是无法让对方"归还"，因为需要考虑 Shuffle 过程中的很多因素，实现起来较为复杂；而且 Shuffle 过程产生的文件在后面一定会被使用到。
动态占用机制图示：

![](image\动态占用机制.png)

### 5、Task内存分布

Task 之间内存分布
为了更好地使用使用内存，Executor 内运行的 Task 之间共享着 Execution 内存。具体的，Spark 内部维护了一个 HashMap 用于记录每个 Task 占用的内存。当 Task 需要在 Execution 内存区域申请 numBytes 内存，其先判断 HashMap 里面是否维护着这个 Task 的内存使用情况，如果没有，则将这个 Task 内存使用置为0，并且以 TaskId 为 key，内存使用为 value 加入到 HashMap 里面。之后为这个 Task 申请 numBytes 内存，如果 Execution 内存区域正好有大于 numBytes 的空闲内存，则在 HashMap 里面将当前 Task 使用的内存加上 numBytes，然后返回；如果当前 Execution 内存区域无法申请到每个 Task 最小可申请的内存，则当前 Task 被阻塞，直到有其他任务释放了足够的执行内存，该任务才可以被唤醒。每个 Task 可以使用 Execution 内存大小范围为 1/2N ~ 1/N，其中 N 为当前 Executor 内正在运行的 Task 个数。一个 Task 能够运行必须申请到最小内存为 (1/2N * Execution 内存)；当 N = 1 的时候，Task 可以使用全部的 Execution 内存。

比如如果 Execution 内存大小为 10GB，当前 Executor 内正在运行的 Task 个数为5，则该 Task 可以申请的内存范围为 10 / (2 * 5) ~ 10 / 5，也就是 1GB ~ 2GB的范围。

### 6、python memory

spark.python.worker.memory vs spark.executor.pyspark.memory

在pyspark里，一个executor有两个进程：

一个JVM运行spark 代码（joins,aggregations,shuffles）

一个python进程，运行用户的代码。

两个进程通过Py4j进行通信。

![](image\1_mzfRwPRAiTwi8OdCCSSUQw.png)

https://medium.com/walmartglobaltech/decoding-memory-in-spark-parameters-that-are-often-confused-c11be7488a24#7a05

#### 7、Total Container Memory

![](image\1_8yviZQXq9rXgBoup8W-kHA.png)

一个container管理的总内存=

executor memory + the memory overhead + the python worker memory limit

![](image\yarn后台.png)



### 三、SparkSession配置

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

driver进程的on-heap内存。

#### 9、spark.driver.memoryOverhead

```
config("spark.driver.memoryOverhead","384M") # driverMemory * 0.10, with minimum of 384
```

Amount of non-heap memory to be allocated per driver process in cluster mode, in MiB unless otherwise specified. This is memory that accounts for things like VM overheads, interned strings, other native overheads, etc. This tends to grow with the container size (typically 6-10%). This option is currently supported on YARN, Mesos and Kubernetes. *Note:* Non-heap memory includes off-heap memory (when `spark.memory.offHeap.enabled=true`) and memory used by other driver processes (e.g. python process that goes with a PySpark driver) and memory used by other non-driver processes running in the same container. The maximum memory size of container to running driver is determined by the sum of `spark.driver.memoryOverhead` and `spark.driver.memory`.

默认值是max(DriverMemory*0.1,384m)。在YARN的cluster模式下，driver端申请的off-heap内存的总量，通常是driver堆内存的6%-10%。

#### 10、spark.executor.memory

```
config("spark.executor.memory","1g") # 默认1g
```

Amount of memory to use per executor process, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. `512m`, `2g`).

Executor的jvm内存总量。

参数说明：该参数用于设置每个Executor进程的内存。Executor内存的大小，很多时候直接决定了Spark作业的性能，而且跟常见的JVM OOM异常，也有直接的关联。
参数调优建议：每个Executor进程的内存设置4G-8G较为合适。但是这只是一个参考值，具体的设置还是得根据不同部门的资源队列来定。可以看看自己团队的资源队列的最大内存限制是多少，num-executors乘以executor-memory，就代表了你的Spark作业申请到的总内存量（也就是所有Executor进程的内存总和），这个量是不能超过队列的最大内存量的。此外，如果你是跟团队里其他人共享这个资源队列，那么申请的总内存量最好不要超过资源队列最大总内存的1/3~1/2，避免你自己的Spark作业占用了队列所有的资源，导致别的同学的作业无法运行。

#### 11、spark.executor.pyspark.memory

Not set

它是外部进程内存的一部分，负责python daemon能够使用多少内存。Python守护进程，用于执行python上编写的UDF。

#### 12、spark.executor.memoryOverhead

```
config("spark.executor.memoryOverhead","4096M") # executorMemory * 0.10, with minimum of 384M
```

单个executor申请的off-heap内存的总量。该参数仅仅支持在yarn或者kubernetes上使用，通常可以是executor内存的0.06-0.1。

The maximum memory size of container to running executor is determined by the sum of `spark.executor.memoryOverhead`, `spark.executor.memory`, `spark.memory.offHeap.size` and `spark.executor.pyspark.memory`.

#### 12.1 spark.memory.offHeap.size

The amount of off-heap memory used by Spark to store actual data frames is governed by `spark.memory.offHeap.size`. This is an optional feature, which can be enabled by setting `spark.memory.offHeap.use` to true.

![](image\1_dh9it_gMQai4Zj8NbD3-fA.png)

现在使用spark2.4，所以设置spark.executor.memoryOverhead就足够了。

**For Spark 1.x and 2.x, Total Off-Heap Memory = spark.executor.memoryOverhead (spark.offHeap.size included within)
For Spark 3.x, Total Off-Heap Memory = spark.executor.memoryOverhead + spark.offHeap.size**

#### 13、spark.master

```
.master("yarn")
config("spark.master","yarn")
```

Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.

#### 14、spark.submit.deployMode

```
config("spark.submit.deployMode","cluster") # 默认是client
```

The deploy mode of Spark driver program, either "client" or "cluster", Which means to launch driver program locally ("client") or remotely ("cluster") on one of the nodes inside the cluster.

#### 15、driver日志相关

1、spark.driver.log.dfsDir   

2、spark.driver.log.persistToDfs.enabled 默认为 false

spark 3.x新功能，如果spark.driver.log.persistToDfs.enabled 设置为 True,同时配置日志同步路径，可以把clinnt模式下的driver的日志同步。

3、spark.driver.log.layout  日志输出的格式，如果没有给定，使用log4j.properties配置的格式

```
%d{yy/MM/dd HH:mm:ss.SSS} %t %p %c{1}: %m%n
```



### 3.1 Execution Behavior

#### 1、spark.executor.cores

1 in YARN mode, all the available cores on the worker in standalone and Mesos coarse-grained modes.

参数说明：该参数用于设置每个Executor进程的CPU core数量。这个参数决定了每个Executor进程并行执行task线程的能力。因为每个CPU core同一时间只能执行一个task线程，因此每个Executor进程的CPU core数量越多，越能够快速地执行完分配给自己的所有task线程。
参数调优建议：Executor的CPU core数量设置为2~4个较为合适。同样得根据不同部门的资源队列来定，可以看看自己的资源队列的最大CPU core限制是多少，再依据设置的Executor数量，来决定每个Executor进程可以分配到几个CPU core。同样建议，如果是跟他人共享这个队列，那么num-executors * executor-cores不要超过队列总CPU core的1/3~1/2左右比较合适，也是避免影响其他同学的作业运行。

#### 2、spark.executor.heartbeatInterval

默认=10s

Interval between each executor's heartbeats to the driver.

#### 3、spark.files.useFetchCache

默认=true

If set to true (default), file fetching will use a local cache that is shared by executors that belong to the same application, which can improve task launching performance when running many executors on the same host. If set to false, these caching optimizations will be disabled and all executors will fetch their own copies of files. This optimization may be disabled in order to use Spark local directories that reside on NFS filesystems (see [SPARK-6313](https://issues.apache.org/jira/browse/SPARK-6313) for more details).

```
21/11/16 12:49:56 WARN storage.BlockManager: Block rdd_27_0 already exists on this machine; not re-adding it
```

#### 4、spark.files.maxPartitionBytes

默认=134217728 (128 MiB)

读取文件时打包到单个分区的最大字节数。

#### 5、num-executors

```
--num-executors 1  # 默认=2
```

参数说明：该参数用于设置Spark作业总共要用多少个Executor进程来执行。Driver在向YARN集群管理器申请资源时，YARN集群管理器会尽可能按照你的设置来在集群的各个工作节点上，启动相应数量的Executor进程。这个参数非常之重要，如果不设置的话，默认只会给你启动少量的Executor进程，此时你的Spark作业的运行速度是非常慢的。
参数调优建议：每个Spark作业的运行一般设置50~100个左右的Executor进程比较合适，设置太少或太多的Executor进程都不好。设置的太少，无法充分利用集群资源；设置的太多的话，大部分队列可能无法给予充分的资源。

As for --num-executors flag, you can even keep it at a very high value of 1000. It will still allocate only the number of containers that is possible to launch on each node. As and when your cluster resources increase your containers attached to your application will increase. The number of containers that you can launch per node will be limited by the amount of resources allocated to the nodemanagers on those nodes.

所以这个还是来自动态分配。



### 3.2 Runtime Environment

#### 1、spark.driver.extraClassPath

```
spark submit 使用下面两种方式
----driver-class-path guava-12.0.1.jar
--conf spark.driver.extraClassPath guava-12.0.1.jar
```

加载驱动包

Extra classpath entries to prepend to the classpath of the driver.
*Note:* In client mode, this config must not be set through the `SparkConf` directly in your application, because the driver JVM has already started at that point. Instead, please set this through the `--driver-class-path` command line option or in your default properties file.

#### 2、spark.driver.defaultJavaOptions

```
--driver-java-options # 如果是client模式
 --driver-java-options "-Xms2G -Doracle.jdbc.Trace=true -Djava.util.logging.config.file=/opt/apache-spark/spark-2.3.0-bin-hadoop2.7/conf/oraclejdbclog.properties -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=1098 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true -Djava.rmi.server.hostname=192.168.2.120 -Dcom.sun.management.jmxremote.rmi.port=1095" 
```

修改JVM的参数，可以-Xms，JVM的最小内存。但不能设置-Xmx，JVM最大内存，这个值在集群模式下使用spark.driver.memory；在client模式下，使用--driver-memory在命令行提交。

#### 3、spark.driver.extraJavaOptions

```
--conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
```

设定方法：一般可以不设置。如果设置，常见的情景是使用-Xmn加大年轻代内存的大小，或者手动指定垃圾收集器（最上面的例子中使用了G1，也有用CMS的时候）及其相关参数。

二者的区别和联系

#### 4、spark.driver.userClassPathFirst

Whether to give user-added jars precedence over Spark's own jars when loading classes in the driver. This feature can be used to mitigate conflicts between Spark's dependencies and user dependencies. It is currently an experimental feature. This is used in cluster mode only.

用户自己的jar包覆盖spark的

#### 5、spark.executor.extraClassPath

spark旧版本中的

#### 6、spark.executor.defaultJavaOptions

#### 7、spark.executor.extraJavaOptions

For example, to enable verbose gc logging to a file named for the executor ID of the app in /tmp, pass a 'value' of: `-verbose:gc -Xloggc:/tmp/-.gc` `spark.executor.defaultJavaOptions` will be prepended to this configuration.

#### 8、executor日志

```
spark.executor.logs.rolling.maxRetainedFiles
spark.executor.logs.rolling.enableCompression 
spark.executor.logs.rolling.strategy  time和 size两种
For "time", use spark.executor.logs.rolling.time.interval to set the rolling interval. For "size", use spark.executor.logs.rolling.maxSize to set the maximum file size for rolling.
spark.executor.logs.rolling.maxSize # 如果是size，设置最大的size
spark.executor.logs.rolling.time.interval # 如果根据time，则需要设置时间间隔。默认是daily
```

#### 9、spark.executorEnv.[EnvironmentVariableName]

https://stackoverflow.com/questions/36054871/spark-executorenv-doesnt-seem-to-take-any-effect

#### 10、spark.python.worker.memory

Amount of memory to use per python worker process during aggregation, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. `512m`, `2g`). If the memory used during aggregation goes above this amount, it will spill the data into disks.

"spark.python.worker.memory is used for Python worker in executor" 。JVM中python worker的内存

#### 11、spark.python.worker.reuse

默认为true,复用python worker。

It will be very useful if there is a large broadcast, then the broadcast will not need to be transferred from JVM to Python worker for every task.

task复用 python worker

#### 12、spark.files

```
--files
```



#### 13、spark.submit.pyFiles

```
--pyfiles
```

Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps. Globs are allowed.

#### 14、spark.jars

```
--jars
```

#### 15、spark.pyspark.driver.python

默认为 spark.pyspark.python

#### 16、spark.pyspark.python

driver和executor使用的python版本

```
spark-submit --master yarn   --conf spark.pyspark.python=/usr/bin/python 
```

### 3.3 Shuffle Behavior

#### 1、spark.shuffle.io.maxRetries

默认=3

(Netty only) Fetches that fail due to IO-related exceptions are automatically retried if this is set to a non-zero value. This retry logic helps stabilize large shuffles in the face of long GC pauses or transient network connectivity issues.

#### 2、spark.shuffle.io.retryWait

默认=5s

(Netty only) How long to wait between retries of fetches. The maximum delay caused by retrying is 15 seconds by default, calculated as `maxRetries * retryWait`.

### 3.4 spark UI

#### 1、spark.eventLog.dir

```
file:///tmp/spark-events 默认位置。如果spark.eventLog.enabled=ture
```

#### 2、spark.eventLog.enabled

```
默认为false
```

Whether to log Spark events, useful for reconstructing the Web UI after the application has finished.

### 3、spark.ui.enabled

```
默认为ture。是否开启spark Ui
```

#### 4、spark.ui.killEnabled

```
默认为ture。是否可以在spark Ui页面杀死job 和stage
```

#### 5、spark.ui.port

```
4040 默认端口，如果被使用，其=4041
```

#### 6、保留多个xx

```
spark.ui.retainedJobs = 1000
spark.ui.retainedStages = 1000
spark.ui.retainedTasks = 10000 How many tasks in one stage the Spark UI and status APIs remember
spark.worker.ui.retainedExecutors = 1000
spark.worker.ui.retainedDrivers = 1000
spark.sql.ui.retainedExecutions = 1000 How many finished executions the Spark UI and status APIs remember before garbage collecting

```

总结：当提交spark任务之后，在spark UI界面查看任务的运行情况。

### 3.4 Memory Management

#### 1、spark.memory.fraction

默认=0.6

堆内存空间 0.6 用于执行和存储。

#### 2、spark.memory.storageFraction

默认=0.5

spark.memory.fraction分配的内存中，有多少用于storage

#### 3、spark.memory.offHeap.enabled

默认为 false

If true, Spark will attempt to use off-heap memory for certain operations. If off-heap memory use is enabled, then `spark.memory.offHeap.size` must be positive.

使用堆外内存

#### 4、spark.memory.offHeap.size

默认=0

![](image\1_QiEGPjaFlLgACMKhT0hboQ.png)

The amount of off-heap memory used by Spark to store actual data frames is governed by `spark.memory.offHeap.size`. This is an optional feature, which can be enabled by setting `spark.memory.offHeap.use` to true.

#### 5、spark.storage.replication.proactive

默认为false

Enables proactive block replication for RDD blocks. Cached RDD block replicas lost due to executor failures are replenished if there are any existing available replicas. This tries to get the replication level of the block to the initial number.

#### 6、spark.cleaner.periodicGC.interval

默认=30min

控制触发垃圾收集的频率。

#### 7、spark.cleaner.referenceTracking

默认=true

Enables or disables context cleaning.

#### Cleaner的创建

SparkContext在初始化时就会创建并启动一个cleaner

Spark在这个cleaner中启动了一个定时做垃圾回收单线程`context-cleaner-periodic-gc`

```
spark.cleaner.referenceTracking.blocking=true 表示清理线程是否等待远端操作的完成，即rpc的返回
spark.cleaner.referenceTracking.blocking.shuffle=false 表示shuffule清理线程是否等待远端操作的完成，即rpc的返回
spark.cleaner.referenceTracking.cleanCheckpoints=false 表示当reference  out of scope之后，是否清理checkpoint files
```

cleaner清理的逻辑都在`keepCleaning()`方法中，当RDD被GC回收后，*referenceQueue*会收到删除对象的*reference*，该方法不断从队列中remove *reference*，然后执行真正的清理 **doCleaupXXX()**

```
/** Keep cleaning RDD, shuffle, and broadcast state. */
  private def keepCleaning(): Unit = Utils.tryOrStopSparkContext(sc) {
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            referenceBuffer.remove(ref)
            ref.task match {
              case CleanRDD(rddId) =>
                doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
              case CleanShuffle(shuffleId) =>
                doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
              case CleanBroadcast(broadcastId) =>
                doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
              case CleanAccum(accId) =>
                doCleanupAccum(accId, blocking = blockOnCleanupTasks)
              case CleanCheckpoint(rddId) =>
                doCleanCheckpoint(rddId)
            }
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

```



### 四、spark环境变量

```
conf/spark-env.sh
```

spark安装之后，spark-env.sh并不存在。通过  conf/spark-env.sh.template来创建。

设置如下环境变量

| Environment Variable    | Meaning                                                      |
| :---------------------- | :----------------------------------------------------------- |
| `JAVA_HOME`             | Location where Java is installed (if it's not on your default `PATH`). |
| `PYSPARK_PYTHON`        | Python binary executable to use for PySpark in both driver and workers (default is `python3` if available, otherwise `python`). Property `spark.pyspark.python` take precedence if it is set |
| `PYSPARK_DRIVER_PYTHON` | Python binary executable to use for PySpark in driver only (default is `PYSPARK_PYTHON`). Property `spark.pyspark.driver.python` take precedence if it is set |
| `SPARK_LOCAL_IP`        | IP address of the machine to bind to.                        |
| `SPARK_PUBLIC_DNS`      | Hostname your Spark program will advertise to other machines. |

#### 1、查看本机使用的是哪个spark

查看环境变量，

```
echo $PATH # 输出如下
”//usr/local/python3.6/bin:/usr/local/bin:/root/.cargo/bin:/home/dev/golang/go/bin:/home/soft/install/spark-2.4.7-bin-hadoop2.7/bin:/home/soft/maven/apache-maven-3.8.1/bin:/usr/scala/scala-2.11.8/bin:/usr/lib64/qt-3.3/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/root/bin”
```

可以看到，使用的spark安装目录在 `/home/soft/install/spark-2.4.7-bin-hadoop2.7/`

#### 2、查看器spark-env.sh

```
SPARK_DAEMON_MEMORY=5g
# 确定历史日志
export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18082 -Dspark.history.fs.logDirectory=hdfs://nameservice1/user/appuser/applicationHistory"
# yarn配置地址
YARN_CONF_DIR=/opt/cloudera/parcels/CDH/lib/hive/conf/
# hadoop配置地址
HADOOP_CONF_DIR=/opt/cloudera/parcels/CDH/lib/hive/conf/                                                       
```

JAVA_HOME已经在PATH中进行了设置

PYSPARK_PYTHON 在`spark-sumit`的时候通过spark.pyspark.python设置。`PYSPARK_DRIVER_PYTHON`也使用该值

```
spark-submit --master yarn   --conf spark.pyspark.python=/usr/bin/python  main.py
```

SPARK_DAEMON_MEMORY=5g 的含义

![](image\Screen Shot 2014-11-25 at 4.14.49 PM.png)

master和worker守护进程的内存。在master节点，可以看到下面的java进程

```
-Xms2g -Xmx2g org.apache.spark.deploy.master.Master
```

在worker节点

```
-Xms1g -Xmx1g org.apache.spark.deploy.worker.Worker spark://<IP of master>:7077
```



### 3、yarn cluster

在yarn 集群模式下。环境变量通过

```
spark.yarn.appMasterEnv.[EnvironmentVariableName]
```

 In `cluster` mode this controls the environment of the Spark driver.

in `client` mode it only controls the environment of the executor launcher.