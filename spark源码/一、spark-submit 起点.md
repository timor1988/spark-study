## 一、spark-submit 起点

```
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master yarn \
./examples/jars/spark-examples_2.12-3.0.0.jar \
10
```

会产生一个 SparkSubmit进程

JVM---> Process(SparkSubmit)

开始执行 SparkSubmit.main()

最后就会调用：spark-class2.cmd

```
java org.apache.spark.deploy.SparkSubmit
```

shift + shfit 全局搜索进入SparkSubmit.scala文件，ctrl + f  搜索 `main` 找到main方法入口

```
/**
 * This entry point is used by the launcher library to start in-process Spark applications.
 */
private[spark] object InProcessSparkSubmit {

  def main(args: Array[String]): Unit = {
    val submit = new SparkSubmit()
    ...
    submit.doSubmit(args)   
  }

}
```

args 即为命令行参数

## 二、参数解析

```
 val appArgs = parseArguments(args)
```

参数解析方法详情  SparkSubmitArguments：

```
 parse(args.asJava)
```

通过java的正则表达式对参数进行拆解和获取，然后`handle(name,value)`，该方法在 SparkSubmitArguments类中。

完成对类中各个属性赋值: name,master,mainClass,deployMode...

```
/** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case NAME =>
        name = value

      case MASTER =>
        master = value
      ...  
```

### action

其中action属性的值默认为SUBMIT所以是之间提交应用。

```
 action = Option(action).getOrElse(SUBMIT)
```

该参数在SparkSubmit类中用于确定SparkSubmitAction事件

```
   appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs, uninitLog)
      case SparkSubmitAction.KILL => kill(appArgs)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
      case SparkSubmitAction.PRINT_VERSION => printVersion()
    }
  }
```

## 三、doRunMain()

因为没有proxyUser，所以其执行的方法为runMain()

```
 def doRunMain(): Unit = {
     ... 
     runMain(args, uninitLog)
      
    }
```

### 3.1 mainClass

提交环境准备好之后，才会运行下面的代码，此时childMainClass = "org.apache.spark.deploy.yarn.YarnClusterApplication"   

通过反射根据类名得到类。然后判断：如果继承了SparkApplication，则得到对应的实例。否则，new一个。

```
var mainClass: Class[_] = null
      ...
mainClass = Utils.classForName(childMainClass)
... 
 val app: SparkApplication = if (classOf[SparkApplication].isAssignableFrom(mainClass)) {
      mainClass.getConstructor().newInstance().asInstanceOf[SparkApplication]
    } else {
      new JavaMainApplication(mainClass)
    }
```



## 四、prepareSubmitEnvironment

准备提交环境

```
val (childArgs, childClasspath, sparkConf, childMainClass) = prepareSubmitEnvironment(args)
```

### 4.1 childMainClass

提交环境为yarn集群环境

```
 if (isYarnCluster) {
      childMainClass = YARN_CLUSTER_SUBMIT_CLASS
 private[deploy] val YARN_CLUSTER_SUBMIT_CLASS =
    "org.apache.spark.deploy.yarn.YarnClusterApplication"      
```

所以

```
childMainClass = "org.apache.spark.deploy.yarn.YarnClusterApplication"   
```

### 

## 五、提交ApplicationMaster

sparksubmit 里面得到application 即  3.1中的app之后，会调用start方法。

```
  app.start(childArgs.toArray, sparkConf)
```

这里会执行 client.scala中的`YarnClusterApplication`类的start方法  。ps : shift+shfit 全局搜索`org.apache.spark.deploy.yarn.YarnClusterApplication` 得到。

然后执行new Client，此时，执行 new ClientArguments

```
 override def start(args: Array[String], conf: SparkConf): Unit = {
    // SparkSubmit would use yarn cache to distribute files & jars in yarn mode,
    // so remove them from sparkConf here for yarn mode.
    conf.remove(JARS)
    conf.remove(FILES)

    new Client(new ClientArguments(args), conf, null).run()
  }
```

这里面的args即为四中得到的`childArgs`

conf为四中得到的`sparkConf`

### 5.2 Client类

属性1：yarnClient

```
private val yarnClient = YarnClient.createYarnClient
```

会创建一个YarnClient

```
public static YarnClient createYarnClient() {
    YarnClient client = new YarnClientImpl();
    return client;
  }
```

### 5.3 Client类的run()方法

向ResourceManager提交应用

```
/**
   * Submit an application to the ResourceManager.
   * If set spark.yarn.submit.waitAppCompletion to true, it will stay alive
   * reporting the application's status until the application has exited for any reason.
   * Otherwise, the client process will exit after submission.
   * If the application finishes with a failed, killed, or undefined status,
   * throw an appropriate SparkException.
   */
  def run(): Unit = {
    this.appId = submitApplication()
```

appId为 yarn的全局应用id

在submitApplication方法里面

```
 launcherBackend.connect()
 yarnClient.init(hadoopConf)
 yarnClient.start()
```

yarnClient启动。

然后继续执行，通过yarnClient创建一个应用。

```
/ Get a new application from our RM
val newApp = yarnClient.createApplication()
val newAppResponse = newApp.getNewApplicationResponse()
appId = newAppResponse.getApplicationId()
```

然后设置容器环境和提交的应用

```
  // Set up the appropriate contexts to launch our AM
  val containerContext = createContainerLaunchContext(newAppResponse)
  val appContext = createApplicationSubmissionContext(newApp, containerContext)
```

在 createApplicationSubmissionContext 里面，会执行新的/bin/java，启动一个进程

```
/bin/java   amArgs 
```

其中amArgs里面的

```
amClass = "org.apache.spark.deploy.yarn.ApplicationMaster"
```

即执行的进程其实是

```
/bin/java org.apache.spark.deploy.yarn.ApplicationMaster
```

这个命令封装放在容器中。最后返回这个容器

```
amContainer.setCommands(printableCommands.asJava)
```

所谓的提交：封装了一些指令的容器提交给ResourceManger。

ResourceManager收到这些指令之后，会在某个NodeManager启动 ApplicationMaster。最后提交并监控应用

```
// Finally, submit and monitor the application
  yarnClient.submitApplication(appContext)
```

综上所述：ApplicationMaster已经提交到了某个节点

## 六、 ApplicationMaster 启动运行

### 6.1 启动Driver线程

ApplicationMaster是一个进行，所以有main方法。全局搜索`org.apache.spark.deploy.yarn.ApplicationMaster`。找到其main方法

```
  def main(args: Array[String]): Unit = {
    SignalUtils.registerLogger(log)
    val amArgs = new ApplicationMasterArguments(args)
    val sparkConf = new SparkConf()
    if (amArgs.propertiesFile != null) {
      Utils.getPropertiesFromFile(amArgs.propertiesFile).foreach { case (k, v) =>
        sparkConf.set(k, v)
      }
    }
```

1、ApplicationMasterArguments(args)

对命令行参数进行封装

```
... 
 case ("--class") :: value :: tail =>
          userClass = value
          args = tail
...          
```

2、master 

```
master = new ApplicationMaster(amArgs, sparkConf, yarnConf)
```

里面生成 一个YarnRMClient，即ApplicationMaster和ResourceManager进行交互的客户端

```
private val client = new YarnRMClient()
	  // YarnRMClient类包含的属性
      private var amClient: AMRMClient[ContainerRequest] = _
```

得到master之后，会执行其run方法

```
override def run(): Unit = System.exit(master.run())
```

run方法的主要代码段：

```
 if (isClusterMode) {
        runDriver()
      } else {
        runExecutorLauncher()
      }
```

### 6.1.2 runDriver()

因为是cluster模式，执行runDriver()。runDriver方法的主要代码1：

```
  userClassThread = startUserApplication()
```

startUserApplication()方法的主要代码1：

```
  val mainMethod = userClassLoader.loadClass(args.userClass)
      .getMethod("main", classOf[Array[String]])
```

使用类加载器加载userClass，其值来自提交参数中的`--class`。然后从这个类找到main方法

startUserApplication()方法的主要代码2：

```
 userThread.setContextClassLoader(userClassLoader)
    userThread.setName("Driver")
    userThread.start()
    userThread
```

启动一个叫Driver的线程。启动了之后会执行run方法，在run方法里调用前面找到的main方法。等于执行了WordCount

```
  override def run(): Unit = {
  	mainMethod.invoke(null, userArgs.toArray)
```

![](image\Snipaste_2021-11-25_18-49-24.png)

相当于执行到了 `sc = new SparkContext(sparkConf)`

```
object Spakr01_WordCount {
  def main(args: Array[String]): Unit = {
    // Application
    // Spark框架
    // TODO 建立和Spark框架的连接
    // JDBC : Connection
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)
```



runDriver方法主要代码段2

```
 val sc = ThreadUtils.awaitResult(sparkContextPromise.future,
        Duration(totalWaitTime, TimeUnit.MILLISECONDS))
```

开启线程等待，即等待上下文环境对象。所以代码1中要完成这个环境对象，否则到这里会一直等待。

到了这一步。上下文环境已经准备完成。

### 6.2 启动Executor进程

runDriver方法主要代码段3:

```
  registerAM(host, port, userConf, sc.ui.map(_.webUrl), appAttemptId)
```

ApplicationMaster启动之后，需要向yarn申请资源。和ResourceManager进行交互，就需要向ResourceManager注册自己。

runDriver方法主要代码段4:

```
createAllocator(driverRef, userConf, rpcEnv, appAttemptId, distCacheConf)
	// 该方法细节如下
	// 1、通过YarRMClient创建一个分配器
	allocator = client.createAllocator(
      yarnConf,
      _sparkConf,
      appAttemptId,
      driverUrl,
      driverRef,
      securityMgr,
      localResources)
    //2、得到可分配的资源。返回可用资源列表   
    allocator.allocateResources()
```

其中 allocator.allocateResources()核心代码：

```
//前面可知，资源是以container的方式给yarn的，这里就获得这些可分配的容器
val allocatedContainers = allocateResponse.getAllocatedContainers()
//如果可分配的容器个数大于0，比如有20个。则把这些容器关系进行分类，比如是不是在同一台主机，
if (allocatedContainers.size > 0) {
      handleAllocatedContainers(allocatedContainers.asScala)
    }
```

<img src="image\clipboard.png" style="zoom:75%;" />

执行的task发给1还是9呢，就需要选择。



handleAllocatedContainers 核心代码1: 分配完成之后，开始运行已经分配好的容器。

```
  runAllocatedContainers(containersToUse)
```

  runAllocatedContainers核心代码1：可以使用的容器，挨个遍历

```
 for (container <- containersToUse) {
      executorIdCounter += 1
      ...
      // 如果正在运行的executor少于需要的executor则 线程池启动executor
      if (runningExecutors.size() < targetNumExecutors) {
         ...
         launcherPool.execute(()
```

关于启动executor

把需要的配置参数传进去，

```
new ExecutorRunnable(
                Some(container),
                conf,
                sparkConf,
                driverUrl,
                executorId,
                executorHostname,
                executorMemory,
                executorCores,
                appAttemptId.getApplicationId.toString,
                securityMgr,
                localResources,
                ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID // use until fully supported
              ).run()
    // 其中run方法里创建了nodemanager Client。创建了和某个节点的关联。然后启动容器
    nmClient.init(conf)
    nmClient.start()
    startContainer()
    // 其中startContainer方法核心代码如下
    nmClient.startContainer(container.get, ctx)
```

其中ctx为环境信息，其包含了commands。

启动容器之前，需要准备指令。即在容器中需要执行的脚本命令:

```
val commands = prepareCommand()
ctx.setCommands(commands.asJava)
```

prepareCommand方法的核心代码：启动`org.apache.spark.executor.YarnCoarseGrainedExecutorBackend`进程，即executor通信后台。

```
/bin/java org.apache.spark.executor.YarnCoarseGrainedExecutorBackend
```

继续 shift+ shift 查看`org.apache.spark.executor.YarnCoarseGrainedExecutorBackend`

### 6.3 真正的创建executor计算对象

其main方法里主要代码为执行run()方法

```
 CoarseGrainedExecutorBackend.run(backendArgs, createFn)
```

run方法核心代码1：和Driver取得关联

```
 // Bootstrap to fetch the driver's Spark properties.
 val executorConf = new SparkConf
 val fetcher = RpcEnv.create(
 "driverPropsFetcher",
   ...
```

run方法核心代码2：创建executor运行时的环境。同时把创建出来的对象设置为名为`executor`的通信终端

```
val env = SparkEnv.createExecutorEnv(driverConf, arguments.executorId, arguments.bindAddress,
        arguments.hostname, arguments.cores, cfg.ioEncryptionKey, isLocal = false)
env.rpcEnv.setupEndpoint("Executor",
        backendCreateFn(env.rpcEnv, arguments, env, cfg.resourceProfile))
```

其中 backendCreateFn = createFn，main方法里里面创建的 `CoarseGrainedExecutorBackend`

```
 val createFn: (RpcEnv, CoarseGrainedExecutorBackend.Arguments, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = { case (rpcEnv, arguments, env, resourceProfile) =>
      new YarnCoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl, arguments.executorId,
        arguments.bindAddress, arguments.hostname, arguments.cores, arguments.userClassPath, env,
        arguments.resourcesFileOpt, resourceProfile)
    }
```

<img src="image\Snipaste_2021-11-25_19-54-38.png"  />

#### 1. setupEndpoint

最终执行的代码在： NettyRpcEnv.scala的 NettyRpcEnv类中的setupEndpoint方法

```
private[netty] class NettyRpcEnv(
    val conf: SparkConf,
    javaSerializerInstance: JavaSerializerInstance,
    host: String,
    securityManager: SecurityManager,
   ...
  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }
```

setupEndpoint核心代码1：创建通信地址和通信引用

```
 def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
```

setupEndpoint核心代码2：A message loop that is dedicated to a single RPC endpoint。用于RPC终端的一个message loop

```
 messageLoop = endpoint match {
          case e: IsolatedRpcEndpoint =>
            new DedicatedMessageLoop(name, e, this)
```

DedicatedMessageLoop 核心代码1：Inbox：指令消息收件箱。一个本地 RpcEndpoint 对应一个收件箱，Dispatcher 在每次向Inbox 存入消息时，都将对应 EndpointData 加入内部 ReceiverQueue 中，另外 Dispatcher创建时会启动一个单独线程进行轮询 ReceiverQueue，进行收件箱消息消费；

```
private val inbox = new Inbox(name, endpoint)
```

在 Spark 中，所有的终端都存在生命周期：Constructor、onStart、receive*、onStop。所以 Inbox类中会执行

```
 inbox.synchronized {
    messages.add(OnStart)
  }
```

发送Onstart给自己，这时候`CoarseGrainedExecutorBackend`对象开始执行onStart方法：拿到Driver，并发送一个请求。请求注册当前的executor。

#### 向Driver发送请求

```
override def onStart(): Unit = {
	rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      driver = Some(ref)
      ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls,
        extractAttributes, _resources, resourceProfile.id))
    }(ThreadUtils.sameThread).onComplete {
      case Success(_) =>
        self.send(RegisteredExecutor)
      case Failure(e) =>
        exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
    }(ThreadUtils.sameThread)
```

如果Driver回复了ture，代表注册成功了。则给自己发消息已经注册了。

然后它自己再收到这个消息，前面的是用来通信的，现在才是真正的创建了一个Executor

```
 override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor =>
        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false,resources = _resources)
        driver.get.send(LaunchedExecutor(executorId))
```

![](image\Snipaste_2021-11-25_20-30-23.png)





#### Driver的 SparkContext接收请求

SparkContext中有` SchedulerBackend`为通信后台，和`CoarseGrainedExecutorBackend`进行对接

```
private var _schedulerBackend: SchedulerBackend = _
```

其核心代码：对`RegisterExecutor`请求进行处理和回复。

```
override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

      case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls,
          attributes, resources, resourceProfileId) =>
          .... 增加executorId,增加总核数，增加executor的数量
          addressToExecutorId(executorAddress) = executorId
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)
          ... 给executor回复了一个true。
           context.reply(true)
```



DedicatedMessageLoop 核心代码2：

```
```

#### 2 创建计算对象

回到runDriver() 。此时环境以及准备完成。继续执行下面的代码：让Driver继续执行。

在此之前的操作都是和环境相关，后面的是和计算相关。

即 让 WordCount的代码继续往下运行

```
resumeDriver()
```

告诉你准备工作已经完成：

```
// Post init
_taskScheduler.postStartHook()
```

最终代码在:YarnClusterScheduler类里，执行了`ApplicationMaster.sparkContextInitialized(sc)`之后，runDriver()才会继续往后执行。

```
override def postStartHook(): Unit = {
    ApplicationMaster.sparkContextInitialized(sc)
    super.postStartHook()
    logInfo("YarnClusterScheduler.postStartHook done")
  }
```

其中`super.postStartHook()`会让程序进入等待，直到runDriver()执行到：

```
  resumeDriver()
  // 里面执行，让程序停止等待。
   sparkContextPromise.notify()
```

## 开始执行job

