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

