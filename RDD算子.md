### 1 map

```
rdd = rdd.map(lambda x: funx(x))
```

### 2 mapPartitions

与map方法类似，map是对rdd中的每一个元素进行操作，而mapPartitions(foreachPartition)则是对rdd中的每个分区的迭代器进行操作。如果在map过程中需要频繁创建额外的对象(例如将rdd中的数据通过jdbc写入数据库,map需要为每个元素创建一个链接而mapPartition为每个partition创建一个链接),则mapPartitions效率比map高的多。

会将整个分区的整个数据加载到内存中，处理完的数据不会被释放，因为存在引用。在内存较小，数据量较大的情况下，容易内存溢出。

对比map，来一条处理一条，处理完就释放。

```
x = sc.parallelize([1,2,3], 2)
def f(iterator): 
	yield sum(iterator)
y = x.mapPartitions(f).collect() # [3,3]
# glom() flattens elements on the same partition
print(x.glom().collect())  # [[1], [2, 3]]
print(y.glom().collect()) # [[1], [5]]
```

注意事项：

1、f函数中的返回值需要为一个迭代器

2、mapPartitions 传入的数据，只等循环迭代一次，如果需要二次循环，需要在第一次循环的过程中，新建一个新的列表保存该分区的数据。

```
def func_partitions(items);
	new_item [ ]
	for item in items:
		new_item.append(item)
		do_something(item)
```

### 3 flatMap

相当于每个元素map之后得到一个列表，但是列表的元素为一个元组，flatMap 再把元组拆拆开，最后的列表，每一个元素为一个原子数据。

```
x = sc.parallelize(["hello world","kaka haha","timor teemo"])
print(x.collect())
y = x.flatMap(lambda x: x.split(" "))
print(y.collect())
```

### 4 glom

将每个分区的数据，变成列表形式返回

```
rdd = sc.parallelize([1, 2, 3, 4], 2)
print sorted(rdd.glom().collect()) # [[1, 2], [3, 4]]
```





## 二、key-value类型

### 1、partitionBy

将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner

```
rdd = sc.parallelize([(1,2),(2,3),(3,4),(6,4),(5,1),(4,5)],2)
rdd = rdd.partitionBy(3, partitionFunc=lambda x: int(x))
print(rdd.glom().collect())
```

输入的x是指 k-v数据中的key。如何确定key被分到哪个分区，根据Partitioner类

```
![reducebykey](C:\Users\18177\Desktop\javaprojects\spark-study\image\reducebykey.png)class Partitioner(object):
    def __init__(self, numPartitions, partitionFunc):
        self.numPartitions = numPartitions
        self.partitionFunc = partitionFunc

    def __eq__(self, other):
        return (isinstance(other, Partitioner) and self.numPartitions == other.numPartitions
                and self.partitionFunc == other.partitionFunc)

    def __call__(self, k):
        return self.partitionFunc(k) % self.numPartitions
        
part = Partitioner(3,partitionFunc= lambda x:int(x))
print(part(1),part(2),part(3),part(4),part(5),part(6))        
```

实验可以发现，两个print的输出如下：3，6对3取余数=0，所以在0号分区。其它key同理

```
1 2 0 1 2 0
[[(3, 4), (6, 4)], [(1, 2), (4, 5)], [(2, 3), (5, 1)]]
```

### 2、reduceByKey

将数据按照相同的 Key 对 Value 进行聚合

```
rdd = sc.parallelize([(1,2),(1,3),(1,4),(2,4),(2,1),(2,5),(3,1)],2)
rdd = rdd.reduceByKey(lambda x,y: x+y)
```

![]()![reducebykey](image\reducebykey.png)

### 3、groupByKey

将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组 ，元组中的第一个元素就是key，元组中的第二个元素就是相同key的value的集合

```
rdd = sc.parallelize([(1,2),(1,3),(1,4),(2,4),(2,1),(2,5),(3,1)],2)
rdd = rdd.groupByKey()
print(rdd.collect())
```

[(2, <pyspark.resultiterable.ResultIterable object at 0x000001F0A5763748>), (1, <pyspark.resultiterable.ResultIterable object at 0x000001F0A5763708>), (3, <pyspark.resultiterable.ResultIterable object at 0x000001F0A5763788>)]

可知，元组的第二个元素是<pyspark.resultiterable.ResultIterable 对象，需要转换为python对象。mapValues就可以完成这个转换。

```
rdd = rdd.groupByKey().mapValues(list) 
```



#### groupByKey存在的问题：

shuffle操作必须落盘处理，不能在内存中等待，可能内存溢出。所以涉及磁盘IO，shuffle操作性能很低。groupByKey就是经过了shuffle。

#### reduceByKey的优势

同分区内相同key的就可以在落盘之前先进行聚合了。

分区内预聚合功能，可以有效减少shuffle时候落盘的数据量，提升性能

![](image\red.png)

如果只需要分组，不进行聚合，就只能使用groupByKey。

## 三、RDD 持久化

RDD 通过 Cache 或者 Persist 方法将前面的计算结果缓存，默认情况下会把数据以缓存在 JVM 的堆内存中。但是并不是这两个方法被调用时立即缓存，而是触发后面的 action 算 子时，该 RDD 将会被缓存在计算节点的内存中，并供后面重用。

![](image\持久化.png)

```
mapRDD.persist(StorageLevel.DISK_ONLY)
```

2、RDD CheckPoint 检查点

所谓的检查点其实就是通过将 RDD 中间结果写入磁盘

由于血缘依赖过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果检查点之后有节点出现问题，可以从检查点开始重做血缘，减少了开销。

对 RDD 进行 checkpoint 操作并不会马上被执行，必须执行 Action 操作才能触发。

```
mapRDD.checkpoint()
```

三者区别：

cache : 将数据临时存储在内存中进行数据重用 ，会在血缘关系中添加新的依赖。一旦，出现问题，可以重头读取数据 

persist : 将数据临时存储在磁盘文件中进行数据重用。涉及到磁盘IO，性能较低，但是数据安全。  如果作业执行完毕，临时保存的数据文件就会丢失。

 checkpoint : 将数据长久地保存在磁盘文件中进行数据重用。涉及到磁盘IO，性能较低，但是数据安全 。 为了保证数据安全，所以一般情况下，会独立执行作业 。了能够提高效率，一般情况下，是需要和cache联合使用。执行过程中，会切断血缘关系。重新建立新的血缘关系。checkpoint等同于改变数据源