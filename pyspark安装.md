## 一、win10安装

### 1、安装jdk
jdk-8u241-windows-x64 版本及其以上

环境变量设置：
```
JAVA_HOME
Path
CLASSPATH：xxx\java\lib
```
注意：java安装路径不能有空格。

测试安装成功
`java -version`

### 2、安装spark和hadoop

下载 `spark-2.4.7-bin-hadoop2.7.tgz`

### 2.2 安装 spark2.4.7版本，和服务器版本一致

下载地址：https://archive.apache.org/dist/spark/spark-2.4.7/

然后解压、配置环境变量

```
SPARK_HOME =  D:\software\spark-2.4.7-bin-hadoop2.7
```



```
SPARK_HOME
Path
HADOOP_HOME
Path
```

将`C:\SoftWare\spark-2.4.7-bin-hadoop2.7\python\lib`中的
`pyspark`和`py4j-0.10.9-src`复制到
`C:\Users\18177\Anaconda3\Lib\site-packages`文件夹下，
同时，删除该文件夹下的pyspark 和 py4j文件夹。
解压这两个压缩文件，得到新的pyspark 和 py4j文件夹。解压完成，删除两个压缩文件。





### 3、下载winutil.exe

将winutils.exe文件放到Hadoop的bin目录下（C:\SoftWare\hadoop-2.7.1\bin），然后以管理员的身份打开cmd，然后通过cd命令进入到Hadoop的bin目录下，然后执行以下命令：

winutils.exe chmod 777 c:\tmp\Hive


### 4、pycharm使用pyspark
安装完成，如果在cmd里输入`pyspark`成功，但是pycharm运行失败，把`JAVA_HOME,SPARK_HOME`添加到py文件的环境变量里。或者重启之后，pycharm会加载最新版的系统环境变量。

## 二、linux安装

1、安装java

```
cd /opt/software/
tar -zxvf jdk-8u212-linux-x64.tar.gz -C /opt/module/
```

```
# 添加环境变量
#JAVA_HOME
export JAVA_HOME=/opt/module/jdk1.8.0_212
export PATH=$PATH:$JAVA_HOME/bin
# 使其生效
source /etc/profile
```

2、安装hadoop

```
cd /opt/software/
tar -zxvf hadoop-3.1.3.tar.gz -C /opt/module/
```

```
#HADOOP_HOME 配置环境变量
export HADOOP_HOME=/opt/module/hadoop-3.1.3
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
```



### 1、创建spark目录

```
mkdir /usr/spark
cd /usr/spark
```

### 2、下载spark安装包

```
https://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-2.4.2/spark-2.4.2-bin-hadoop2.7.tgz
```

### 3、解压并配置环境变量

```
tar -zxvf spark-2.4.2-bin-hadoop2.7.tgz
# 配置环境变量
vi /etc/profile
# 在profile文件中最后一行写入：
PATH=/usr/spark/spark-2.4.2-bin-hadoop2.7/bin:$PATH
# 然后使其生效
source /etc/profile
echo $PATH
```

### 4、安装pyspark模块

```
pip install pyspark
```

