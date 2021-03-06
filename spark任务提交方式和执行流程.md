## 一、Spark中的基本概念

（1）Application：表示你的应用程序

（2）Driver：表示main()函数，创建SparkContext。由SparkContext负责与ClusterManager通信，进行资源的申请，任务的分配和监控等。程序执行完毕后关闭SparkContext

（3）Executor：某个Application运行在Worker节点上的一个进程，该进程负责运行某些task，并且负责将数据存在内存或者磁盘上。在Spark on Yarn模式下，其进程名称为 CoarseGrainedExecutor Backend，一个CoarseGrainedExecutor Backend进程有且仅有一个executor对象，它负责将Task包装成taskRunner，并从线程池中抽取出一个空闲线程运行Task，这样，每个CoarseGrainedExecutorBackend能并行运行Task的数据就取决于分配给它的CPU的个数。

（4）Worker：集群中可以运行Application代码的节点。在Standalone模式中指的是通过slave文件配置的worker节点，在Spark on Yarn模式中指的就是NodeManager节点。

（5）Task：在Executor进程中执行任务的工作单元，多个Task组成一个Stage

（6）Job：包含多个Task组成的并行计算，是由Action行为触发的

（7）Stage：每个Job会被拆分很多组Task，作为一个TaskSet，其名称为Stage

（8）DAGScheduler：根据Job构建基于Stage的DAG，并提交Stage给TaskScheduler，其划分Stage的依据是RDD之间的依赖关系

（9）TaskScheduler：将TaskSet提交给Worker（集群）运行，每个Executor运行什么Task就是在此处分配的。

 (10) SchedulerBackend：是一个tait，作用是分配当前可用的资源，具体就是向当前等待分配计算资源的task分配计算资源，即Exector，并且在分配的Exector中启动task，完成计算调度。

![](image\结构图.png)

## 二、spark 模式

### 1、local模式

spark-3.0.0-bin-hadoop3.2tgz解压缩，重命名为spark-local 进入spark-local文件夹

```
bin/spark-shell # 会自动生成一个sparkcontext，所以可以直接使用sc各种方法
sc.textFile("data/word.txt").flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey(_+_).collect()
```

本地提交应用

```
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master local[2] \
./examples/jars/spark-examples_2.12-3.0.0.jar \
10
```

 spark-examples_2.12-3.0.0.jar 运行的应用类所在的 jar 包，实际使用时，可以设定为自己打的 jar 包

### 2、Standalone模式

Spark 的 Standalone 模式体现了经典的 master-slave 模式。

![](image\clipboard.png)

开启步骤：

1、解压缩文件

```
tar -zxvf spark-3.0.0-bin-hadoop3.2.tgz -C /opt/module 
cd /opt/module 
mv spark-3.0.0-bin-hadoop3.2 spark-standalone
```

2、修改配置文件

1) 进入解压缩后路径的 conf 目录，修改 slaves.template 文件名为 slaves

```
mv slaves.template slaves
```

2) 修改 slaves 文件，添加 work 节点

```
hadoop102 hadoop103 hadoop104
```

3) 修改 spark-env.sh.template 文件名为 spark-env.sh

```
mv spark-env.sh.template spark-env.sh
```

4) 修改 spark-env.sh 文件，添加 JAVA_HOME 环境变量和集群对应的 master 节点

```
export JAVA_HOME=/opt/module/jdk1.8.0_144 SPARK_MASTER_HOST=hadoop102 SPARK_MASTER_PORT=7077
```

注意：7077 端口，相当于 hadoop3 内部通信的 8020 端口，此处的端口需要确认自己的 Hadoop配置。

5) 分发 spark-standalone 目录

```
xsync spark-standalone
```

3、启动hadoop集群

1、执行启动脚本

```
sbin/start-all.sh
```

2、查看三台服务器运行进程

```
jps
```

3、查看Master资源监控web UI界面 http://hadoop102:8080

4、提交应用

```
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master spark://hadoop102:7077 \
./examples/jars/spark-examples_2.12-3.0.0.jar \
10
```

执行任务时，会产生多个 Java 进程:

![](image\java进程.png)

### 3 yarn模式

1、解压缩

将 spark-3.0.0-bin-hadoop3.2.tgz 文件上传到 linux 并解压缩，放置在指定位置。

```
tar -zxvf spark-3.0.0-bin-hadoop3.2.tgz -C /opt/module 
cd /opt/module 
mv spark-3.0.0-bin-hadoop3.2 spark-yarn
```

2、1) 修改 hadoop 配置文件/opt/module/hadoop/etc/hadoop/yarn-site.xml, 并分发

```
<!--是否启动一个线程检查每个任务正使用的物理内存量，如果任务超出分配值，则直接将其杀掉，默认 是 true --> 
<property>    
	<name>yarn.nodemanager.pmem-check-enabled</name>    
	<value>false</value> 
</property> 
<!--是否启动一个线程检查每个任务正使用的虚拟内存量，如果任务超出分配值，则直接将其杀掉，默认 是 true --> 
<property>    
	<name>yarn.nodemanager.vmem-check-enabled</name>   
	<value>false</value> 
</property>
```

​	2) 修改 conf/spark-env.sh，添加 JAVA_HOME 和 YARN_CONF_DIR 配置

```
mv spark-env.sh.template spark-env.sh 
...
export JAVA_HOME=/opt/module/jdk1.8.0_144 
YARN_CONF_DIR=/opt/module/hadoop/etc/hadoop
```

3、启动 HDFS 以及 YARN 集群

```
myhadoop.sh
```

4、提交应用

```
bin/spark-submit --class org.apache.spark.examples.SparkPi  --master yarn  --deploy-mode client  ./examples/jars/spark-examples_2.12-3.0.0.jar  10
```

### 四、windows本地模式

执行解压缩文件路径下 bin 目录中的 spark-shell.cmd 文件

资料\spark-3.0.0-bin-hadoop3.2\bin

启动 Spark 本地环境

在bin目录的dos窗口提交任务

```
spark-submit --class org.apache.spark.examples.SparkPi --master local[2] ../examples/jars/spark-examples_2.12-3.0.0.jar 10
```



## 三、Spark 运行流程

### 2.1 Spark on Yarn的Yarn-Client运行过程

 Yarn-Client模式中，Driver在客户端本地运行，这种模式可以使得Spark Application和客户端进行交互，因为Driver在客户端，所以可以通过webUI访问Driver的状态，默认是http://xxx:4040访问，而YARN通过http:// xxx:8088访问。

YARN-client的工作流程分为以下几个步骤：

  （1）Spark Yarn Client向YARN的ResourceManager申请启动Application Master。同时在SparkContent初始化中将创建DAGScheduler和TasKScheduler等，由于我们选择的是Yarn-Client模式，程序会选择YarnClientClusterScheduler和YarnClientSchedulerBackend；

 （2）ResourceManager收到请求后，在集群中选择一个NodeManager，为该应用程序分配第一个Container，要求它在这个Container中启动应用程序的ApplicationMaster，与YARN-Cluster区别的是在该ApplicationMaster不运行SparkContext，只与SparkContext进行联系进行资源的分派；

 （3）Client中的SparkContext初始化完毕后，与ApplicationMaster建立通讯，向ResourceManager注册，根据任务信息向ResourceManager申请资源（Container）；

 （4）一旦ApplicationMaster申请到资源（也就是Container）后，便与对应的NodeManager通信，要求它在获得的Container中启动启动CoarseGrainedExecutorBackend，CoarseGrainedExecutorBackend启动后会向Client中的SparkContext注册并申请Task；

 （5）Client中的SparkContext分配Task给CoarseGrainedExecutorBackend执行，CoarseGrainedExecutorBackend运行Task并向Driver汇报运行的状态和进度，以让Client随时掌握各个任务的运行状态，从而可以在任务失败时重新启动任务；

 （6）应用程序运行完成后，Client的SparkContext向ResourceManager申请注销并关闭自己。

![](image\client模式.png)

### 2.2 Yarn-Cluster运行模式

  在YARN-Cluster模式中，当用户向YARN中提交一个应用程序后，YARN将分两个阶段运行该应用程序：第一个阶段是把Spark的Driver作为一个ApplicationMaster在YARN集群中先启动；第二个阶段是由ApplicationMaster创建应用程序，然后为它向ResourceManager申请资源，并启动Executor来运行Task，同时监控它的整个运行过程，直到运行完成。

 YARN-cluster的工作流程分为以下几个步骤：

1. Spark Yarn Client向YARN中提交应用程序，包括ApplicationMaster程序、启动ApplicationMaster的命令、需要在Executor中运行的程序等；

2. ResourceManager收到请求后，在集群中选择一个NodeManager，为该应用程序分配第一个Container，要求它在这个Container中启动应用程序的ApplicationMaster，其中ApplicationMaster进行SparkContext等的初始化；

3. ApplicationMaster向ResourceManager注册，这样用户可以直接通过ResourceManage查看应用程序的运行状态，然后它将采用轮询的方式通过RPC协议为各个任务申请资源，并监控它们的运行状态直到运行结束；

4. 一旦ApplicationMaster申请到资源（也就是Container）后，便与对应的NodeManager通信，要求它在获得的Container中启动启动CoarseGrainedExecutorBackend，CoarseGrainedExecutorBackend启动后会向ApplicationMaster中的SparkContext注册并申请Task。这一点和Standalone模式一样，只不过SparkContext在Spark Application中初始化时，使用CoarseGrainedSchedulerBackend配合YarnClusterScheduler进行任务的调度，其中YarnClusterScheduler只是对TaskSchedulerImpl的一个简单包装，增加了对Executor的等待逻辑等；

5. ApplicationMaster中的SparkContext分配Task给CoarseGrainedExecutorBackend执行，CoarseGrainedExecutorBackend运行Task并向ApplicationMaster汇报运行的状态和进度，以让ApplicationMaster随时掌握各个任务的运行状态，从而可以在任务失败时重新启动任务；

6. 应用程序运行完成后，ApplicationMaster向ResourceManager申请注销并关闭自己

![](image\cluster模式.png)

### 2.3 YARN-Client 与 YARN-Cluster 区别

理解YARN-Client和YARN-Cluster深层次的区别之前先清楚一个概念：Application Master。在YARN中，每个Application实例都有一个ApplicationMaster进程，它是Application启动的第一个容器。它负责和ResourceManager打交道并请求资源，获取资源之后告诉NodeManager为其启动Container。从深层次的含义讲YARN-Cluster和YARN-Client模式的区别其实就是ApplicationMaster进程的区别。

 1、YARN-Cluster模式下，Driver运行在AM(Application Master)中，它负责向YARN申请资源，并监督作业的运行状况。当用户提交了作业之后，就可以关掉Client，作业会继续在YARN上运行，因而YARN-Cluster模式不适合运行交互类型的作业；

  2、YARN-Client模式下，Application Master仅仅向YARN请求Executor，Client会和请求的Container通信来调度他们工作，也就是说Client不能离开。

## 四、spark-submit 提交任务

### 1、client模式

```
spark-submit --master yarn --executor-memory 6g   --driver-memory 5g --conf spark.pyspark.python=/usr/bin/python main.py
```

--deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
                              on one of the worker machines inside the cluster ("cluster")
                              (Default: client).

### 2、cluster模式

```
spark-submit --master yarn --deploy-mode cluster --executor-memory 6g   --driver-memory 5g --conf spark.pyspark.python=/usr/bin/python main.py
```

