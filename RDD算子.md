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

### 4、aggregateByKey

如果分区内和分区间的聚合规则不一样，就不用能reduceByKey，这时候就使用aggregateByKey。

将数据根据不同的规则进行分区内计算和分区间计算：比如分区内取最大值，分区间求和。

```
val rdd = sc.makeRDD(List(
    ("a", 1), ("a", 2), ("a", 3), ("a", 4)
),2)
// aggregateByKey存在函数柯里化，有两个参数列表
// 第一个参数列表,需要传递一个参数，表示为初始值
//       主要用于当碰见第一个key的时候，和value进行分区内计算
// 第二个参数列表需要传递2个参数
//      第一个参数表示分区内计算规则
//      第二个参数表示分区间计算规则
rdd = rdd.aggregateByKey(0)(
    (x, y) => math.max(x, y),
    (x, y) => x + y
)
```

### 5、foldByKey

当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey。

```
rdd.aggregateByKey(0)(_+_, _+_)
// 如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
rdd.foldByKey(0)(_+_).collect.foreach(println)
```

### 6、combineByKey

最通用的对 key-value 型 rdd 进行聚集操作的聚集函数（aggregation function）。类似于aggregate()，combineByKey()允许用户返回值的类型与输入不一致。

```
val rdd = sc.makeRDD(List(
    ("a", 1), ("a", 2), ("b", 3),
    ("b", 4), ("b", 5), ("a", 6)
),2)

// combineByKey : 方法需要三个参数
// 第一个参数表示：将相同key的第一个数据进行结构的转换，实现操作。
// 第二个参数表示：分区内的计算规则
// 第三个参数表示：分区间的计算规则
val newRDD : RDD[(String, (Int, Int))] = rdd.combineByKey(
    v => (v, 1),  //key="a"的第一个值=1，此时 1=>(1,1)。
    ( t:(Int, Int), v ) => {  
   		 println(t,v)
        (t._1 + v, t._2 + 1)
    },
    (t1:(Int, Int), t2:(Int, Int)) => {
    	println(t1,t2)
        (t1._1 + t2._1, t1._2 + t2._2)
    }
)

val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
    case (num, cnt) => {
        num / cnt
    }
}
```

`v =>(v,1)`实现的是每个分区，每个key第一次出现时候进行转换。其它时候不走这个转换，直接走分区内计算。分区内计算完走分区间计算。

该例中，对于key="a"而言，`("a",1),("a",2)`在一个分区，`("a",6)`在一个分区，所以v经历的转换有：

`1=>(1,1)`和`6=>(6,1)`。

因为这个分区只有一个数据，所以打印分区计算时候，是没有6的。打印`println(t1,t2)`的时候会有`((3,2),(6,1))`，因为分区1经过分区内计算得到了`(3,2)`，直接和`(6,1)`进行分区间计算。

### 7. 四种聚合算子

思考一个问题：reduceByKey、foldByKey、aggregateByKey、combineByKey 的区别？

**reduceByKey**: 相同 key 的第一个数据不进行任何计算，即不进行特殊处理。分区内和分区间计算规则相同

**FoldByKey**: 相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同

**AggregateByKey**：相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同

**CombineByKey**:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。

### 8、join

在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的`(K,(V,W))`的 RDD

join : 两个不同数据源的数据，相同的key的value会连接在一起，形成元组。

如果两个数据源中key没有匹配上，那么数据不会出现在结果中 。

如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积， 数据量会几何性增长，会导致性能降低。

```
val rdd1 = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3)))
val rdd2 = sc.makeRDD(List(("a", 5), ("c", 6),("a", 4)))
val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
```

### 9、leftOuterJoin

左外连接

### 10、rightOuterJoin

右外连接

```
val rdd1 = sc.makeRDD(List(
    ("a", 1), ("b", 2)//, ("c", 3)
))
val rdd2 = sc.makeRDD(List(
    ("a", 4), ("b", 5),("c", 6)
))
//val leftJoinRDD = rdd1.leftOuterJoin(rdd2)
val rightJoinRDD = rdd1.rightOuterJoin(rdd2)
```

### 11、cogroup

在类型为(K,V)和(K,W)的 RDD 上调用，返回一个`(K,(Iterable<V>,Iterable<W>))`类型的 RDD

```
val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5),("c", 6),("c", 7)))
// cogroup : connect + group (分组，连接)
// rdd内根据key聚合之后，再和其它rdd进行连接。
val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
```

cogroup也是根据key进行关联，返回的是JavaPairRDD,JavaPairRDD的两个泛型类型都是Iterable 。

比如本例中第一个RDD元素是(1,"Allen")，(2,"Bob")，(3,"Carl")，第二个RDD元素是(1,10000)，(1,5000)，(2,11000)，(2,6000)，(3,12000)，(3,6000)，

根据key进行cogroup的结果是： (1,([Allen],[10000, 5000])) (2,([Bob],[11000, 6000])) (3,([Carl],[12000, 6000])) 





相当于是两个RDD先在本身内根据key进行关联，将相同key的value放在一个Iterable，然后第一个RDD 会去第二个RDD中寻找key相同的RDD,进行关联，返回新的JavaPairRDD，JavaPairRDD的第一个泛型类型 是之前两个JavaPairRDD的key的类型，因为是根据key进行cogroup的，第二个泛型类型是Tuple2 Tuple2的两个泛型都是Iterable类型，分别是两个RDD中相同key的value形成的Iterable。



## 三、行动算子

### 1、collect collectAsMap()

在驱动程序中，以数组 Array 的形式返回数据集的所有元素

 所谓的行动算子，其实就是触发作业(Job)执行的方法。底层代码调用的是环境对象的runJob方法，底层代码中会创建ActiveJob，并提交执行。

```

```

### 2、reduce

聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据

```
val rdd = sc.makeRDD(List(1,2,3,4))
val i: Int = rdd.reduce(_+_)  // 10
```

### 3、count

返回 RDD 中元素的个数

### 4、first

返回 RDD 中的第一个元素。

真正测试的时候也不用这个，因为得到第一个元素之后，后面的操作就不执行了，`/0`错误就不会被发现。

```
 rdd = sc.parallelize([1,2,3,4,5,6,0])
rdd = rdd.map(lambda x:1/x)
print(rdd.first())
```



### 5、take

返回一个由 RDD 的前 n 个元素组成的数组

### 6、takeOrdered

返回该 RDD 排序后的前 n 个元素组成的数组

```
val rdd1 = sc.makeRDD(List(4,2,3,1))
val ints1: Array[Int] = rdd1.takeOrdered(3)
```

### 7、aggregate

分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合。

aggregateByKey : 初始值只会参与分区内计算

aggregate : 初始值会参与分区内计算,并且也会参与分区间计算

```
val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
// 将该 RDD 所有元素相加得到结果
//val result: Int = rdd.aggregate(0)(_ + _, _ + _)
val result: Int = rdd.aggregate(10)(_ + _, _ + _)
```

### 8、fold

折叠操作，aggregate 的简化版操作。当分区内和分区间使用相同的操作



初始值会参与分区内计算,并且和参与分区间计算

```
val result = rdd.fold(10)(_+_)
```

### 9 、countByKey countByValue

countByKey，统计每种 key 的个数

countByValue，统计值的个数

### 10、save

将数据保存到不同格式的文件中

### 11、foreach

分布式遍历 RDD 中的每一个元素，调用指定函数。是在executor端执行。





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

