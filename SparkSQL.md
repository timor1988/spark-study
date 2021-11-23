## 一、sparkSQL指南

https://spark.apache.org/docs/latest/sql-programming-guide.html

### 1 语法

Spark 支持 SELECT 语句并符合 ANSI SQL 标准。查询用于从一个或多个表中检索结果集。语法如下：

```
[ WITH with_query [ , ... ] ]
select_statement [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select_statement, ... ]
    [ ORDER BY { expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ] } ]
    [ SORT BY { expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ] } ]
    [ CLUSTER BY { expression [ , ... ] } ]
    [ DISTRIBUTE BY { expression [, ... ] } ]
    [ WINDOW { named_window [ , WINDOW named_window, ... ] } ]
    [ LIMIT { ALL | expression } ]
```

其中 `select_statement` 语句为：

```
SELECT [ hints , ... ] [ ALL | DISTINCT ] { [ [ named_expression | regex_column_names ] [ , ... ] | TRANSFORM (...) ] }
    FROM { from_item [ , ... ] }
    [ PIVOT clause ]
    [ LATERAL VIEW clause ] [ ... ] 
    [ WHERE boolean_expression ]
    [ GROUP BY expression [ , ... ] ]
    [ HAVING boolean_expression ]
```



1、from_item 指定查询的输入源。它可以是以下内容之一：

- Table relation
- Join relation
- Table-value function
- Inline table
- Subquery
- File

2、PIVOT 子句用于数据透视；我们可以根据特定的列值得到聚合值。

3、LATERAL VIEW 子句与诸如 EXPLODE 之类的生成器函数结合使用，后者将生成包含一行或多行的虚拟表。横向视图将行应用于每个原始输出行。

4、WHERE 根据提供的谓词筛选 FROM 子句的结果。

5、GROUP BY 指定用于对行进行分组的表达式。它与聚合函数（MIN、MAX、COUNT、SUM、AVG等）结合使用，根据分组表达式和每个组中的聚合值对行进行分组。当 FILTER 子句附加到聚合函数时，只将匹配的行传递给该函数。

6、HAVING 指定筛选 GROUP BY 生成的行所依据的谓词。HAVING子句用于在执行分组后过滤行。如果指定 HAVING 时没有 GROUP BY，则表示 GROUP BY 没有分组表达式（全局聚合）。

7、SORT BY 指定查询的完整结果集的行的顺序。输出行跨分区排序。此参数与SORT BY、CLUSTER BY 和 DISTRIBUTE BY 互斥，不能一起指定。

8、SORT BY：指定在每个分区中对行进行排序的顺序。此参数与 ORDER BY 和CLUSTER BY 互斥，不能一起指定。

9、CLUSTER BY：指定一组用于重新分区和对行排序的表达式。使用此子句与同时使用 DISTRIBUTE BY 和 SORT BY 具有相同的效果。

10、DISTRIBUTE BY：指定用于重新分区结果行的表达式集。此参数与 ORDER BY 和 CLUSTER BY 互斥，不能一起指定。

11、LIMIT：指定语句或子查询可以返回的最大行数。此子句主要与 ORDER BY 结合使用，以产生确定的结果。

12、regex_column_names：当 spark.sql.parser.quotedRegexColumnNames 为true时，SELECT 语句中引用的标识符（使用反勾号```）将被解释为正则表达式，SELECT语句可以采用基于 regex 的列规范。例如，下面的SQL只取c列：

```
 SELECT `(a|b)?+.+` FROM (
     SELECT 1 as a, 2 as b, 3 as c
   )
```

注：这里的正则和re模块有区别。re里这个表达式匹配失败



## 二、win10测试SparkSQL

1、启动Python Console

2、创建sparksession

```
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder. \
    appName("label_recmd_pn"). \
    config("spark.sql.shuffle.partitions", 3). \
    config("spark.default.parallelism", 3). \
    config("spark.python.profile","true").\
    config("spark.sql.parser.quotedRegexColumnNames","true").\
    enableHiveSupport().\
    getOrCreate()
```

3、创建测试表、并插入数据

```
IN[2]: spark.sql("""CREATE TABLE person (id INT, name STRING, age INT)""")
IN[3]: spark.sql("""NSERT INTO person VALUES
    (100, 'John', 30),
    (200, 'Mary', NULL),
    (300, 'Mike', 80),
    (400, 'Dan',  50)""")
```

会在本地建立一个spark-warehouse文件夹，里面有一个person文件夹，再里面有三个文件，因为3个分区。

之后的查询相当于从这里面查。

4、测试sql

```
spark.sql("""SELECT * FROM person WHERE id > 200 ORDER BY id""")
```



## 二、基础

### 1、null值判断

```
# 查找不为空的数据
select pulish_time from a where publish_time is not null
# 查找为空的数据
select pulish_time from a where publish_time is  null
```

### 2、别名

```
select ename AS name, deptno dn from emp
```

### 3、算术运算符

```
select sal +1 from emp
```

### 4、**常用函数**

#### 4.1 count

```
select count(*) cnt from emp 
```

count(*)和count(1)：对表中行数进行统计计算，包含null值。
count(某字段)：对表中该字段的行数进行统计，不包含null值。如果出现空字符串，同样会进行统计。

### 5、limit

LIMIT 子句用于约束 SELECT 语句返回的行数。通常，本子句与 ORDER BY 结合使用，以确保结果具有确定性。

```
select * from emp limit 5;
```

### 6、like 和 rlike

1、使用 **LIKE** **运算选择类似的值**

2、选择条件可以包含字符或数字

% 代表零个或多个字符(任意个字符)。

```
select * from  contents_dwd.dwd_con_video_info_enable WHERE opdate="2021-11-09" and video_title like '%J Balvin,Willy%'
```

_ 代表一个字符。

```
select * from emp where ename like '_A%'; // 第二个字母为A的员工
```

3、RLIKE 子句是 Hive 中这个功能的一个扩展，其可以通过 Java 的正则表达式这个更强大的语言来指定匹配条件

实例：查找名字中带有 A 的员工信息

```
select * from emp where ename RLIKE '[A]'
```

### 7、逻辑运算符:And/Or/Not

```
select * from emp where deptno not IN(30, 20)
```

### 8、**Group By** 

GROUP BY 语句通常会和聚合函数一起使用，按照一个或者多个列队结果进行分组，然后对每个组执行聚合操作。

进行聚合的字段必须出现在select语句中。

GROUP BY 子句用于基于一组指定的分组表达式对行进行分组，并基于一个或多个指定的聚合函数计算行组上的聚合。

Spark 还支持高级聚合，通过 GROUPING SETS, CUBE, ROLLUP 子句对同一输入记录集进行多个聚合。当 FILTER 子句附加到聚合函数时，只将匹配的行传递给该函数。

```
-- 造数据
CREATE TABLE dealer (id INT, city STRING, car_model STRING, quantity INT);
INSERT INTO dealer VALUES
    (100, 'Fremont', 'Honda Civic', 10),
    (100, 'Fremont', 'Honda Accord', 15),
    (100, 'Fremont', 'Honda CRV', 7),
    (200, 'Dublin', 'Honda Civic', 20),
    (200, 'Dublin', 'Honda Accord', 10),
    (200, 'Dublin', 'Honda CRV', 3),
    (300, 'San Jose', 'Honda Civic', 5),
    (300, 'San Jose', 'Honda Accord', 8);
```

查询

```
-- 每个经销商的数量总和. Group by `id`.
SELECT id, sum(quantity)
FROM dealer
GROUP BY id
ORDER BY id;
等价于使用默认列名
SELECT id, sum(quantity)
FROM dealer
GROUP BY 1
ORDER BY 1;
```

#### 8.1 filter条件

将 WHERE 子句中的 boolean_expression 布尔表达式计算为 true 的输入行传递给聚合函数；其他行将被丢弃。

```
-- 每个经销商的“本田思域”和“本田CRV”数量之和
SELECT id, sum(quantity) FILTER (
            WHERE car_model IN ('Honda Civic', 'Honda CRV')
        ) AS `sum(quantity)` FROM dealer
    GROUP BY id ORDER BY id;
```

#### 8.2 grouping sets

```
-- 在一条语句中使用多组分组列的聚合
-- 下面根据四组分组列执行聚合
-- 1. city, car_model
-- 2. city
-- 3. car_model
-- 4. 空分组集，返回所有城市和汽车模型的数量
SELECT city, car_model, sum(quantity) AS sum FROM dealer
    GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
    ORDER BY city;
```

#### 8.3 rollup

在一条语句中指定聚合的多个级别。此子句用于基于多个分组集计算聚合。ROLLUP 是 GROUPING SETS 的简写。例如，`GROUP BY warehouse, product WITH ROLLUP` 相当于 `GROUP BY GROUPING SETS ((warehouse, product), (warehouse), ())`。汇总规范的 N 个元素产生 N+1 个 GROUPING SETS。

```
-- 使用“ROLLUP”子句按处理分组。
-- 等价 GROUP BY GROUPING SETS ((city, car_model), (city), ())
SELECT city, car_model, sum(quantity) AS sum FROM dealer
    GROUP BY city, car_model WITH ROLLUP
    ORDER BY city, car_model
```

#### 8.4 cube

CUBE子 句用于根据 GROUP BY 子句中指定的分组列的组合执行聚合。CUBE 是对GROUPING SETS 的缩写。例如，`GROUP BY warehouse, product WITH CUBE` 相当于 `GROUP BY GROUPING SETS ((warehouse, product), (warehouse), (product), ())`。多维数据集规范的 N 个元素产生 2^N 个分组集。

```
-- 使用“CUBE”子句按处理分组。
-- 等价 GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
SELECT city, car_model, sum(quantity) AS sum FROM dealer
    GROUP BY city, car_model WITH CUBE
    ORDER BY city, car_model
```

#### 8.5 having

HAVING 子句中指定的表达式只能引用：

- 常量 Constants
- GROUP BY 出现的表达式
- 聚合函数 Aggregate functions

```
-- `HAVING` 与 “SELECT” 列表中的聚合函数不同的聚合函数。
SELECT city, sum(quantity) AS sum
FROM dealer
GROUP BY city
HAVING max(quantity) > 15;
```

如果没有gropu by，只进行了统计函数，可以直接用having

```
-- `HAVING` 子句而没有 ` GROUP BY`子句
SELECT sum(quantity) AS sum
FROM dealer
HAVING sum(quantity) > 10;
```

### 9、条件判断

#### 9.1 where

#### 9.2 having

having 与 where 不同点

1、where 后面不能写分组函数，而 having 后面可以使用分组函数。

2、having 只用于 group by 分组统计语句。

3、where 在 group by 之前执行

```
#求每个部门的平均薪水大于 2000 的部门
select deptno, avg(sal) avg_sal from emp group by deptno
having avg_sal > 2000;
```

### 10、join

#### 10.1 join

内连接：只有进行连接的两个表中都存在与连接条件相匹配的数据才会被保留下来。

```
select e.empno, e.ename, d.deptno from emp e join dept d
on e.deptno = d.deptno;
```

#### 10.2 left join

左外连接：JOIN 操作符左边表中符合 WHERE 子句的所有记录将会被返回。

```
select e.empno, e.ename, d.deptno from emp e left join
dept d on e.deptno = d.deptno;
```

注意：当有有分区表的时候，where语句放到on里面，因为where筛选结果，on是关联。

```
select v.video_id,c.collection_id from contents_dim.dim_con_video_movie_relation_2 v
 left join contents_dwd.dwd_content_video_collection_update  c
on (v.video_id =c.video_id and c.opdate="2021-11-01")
```

如果 c.opdate 放到where里面，如果，恰巧 c表为空，则返回结果为空

```
select v.video_id,c.collection_id from contents_dim.dim_con_video_movie_relation_2 v
 left join contents_dwd.dwd_content_video_collection_update  c
on (v.video_id =c.video_id )
where c.opdate="2021-11-01"
```

**取左表独有的数据**

![](image\leftjoin2.png)

```
select e.empno, e.ename, d.deptno from emp e 
left join dept d 
on e.deptno = d.deptno 
where d.deptno is null 
```

首先左连接获取到了左边所有的数据，然后右边为null的情况，就排查了右边出现的记录

不推荐使用 not in 的写法

```
select * from emp e where e.deptno not in (
    select deptno from dept
)
```



#### 10.3 right join

右外连接：JOIN 操作符右边表中符合 WHERE 子句的所有记录将会被返回。

```
select e.empno, e.ename, d.deptno from emp e 
right join dept d 
on e.deptno = d.deptno
```

**取右边独有的数据**

```
select e.empno, e.ename, d.deptno from emp e right join
dept d on e.deptno = d.deptno
where e.empno is null
```

解析同10.2

#### 10.4 full join

满外连接：将会返回所有表中符合 WHERE 语句条件的所有记录。如果任一表的指定字段没有符合条件的值的话，那么就使用 NULL 值替代。全连接

```
#查询所有员工信息和职位信息
select e.empno, e.ename, nvl(e.deptno,d.deptno),d.name from emp e 
full joindept d 
on e.deptno = d.deptno
```

**取左右两表独有的数据**

![](image\fulljoin.png)

```
select e.empno, e.ename, d.deptno from emp e 
full join dept d 
on e.deptno = d.deptno 
where e.deptno is null or d.deptno is null;
```

左边为null时候得到的是右表独有的数据；右边为null的时候得到的是左边独有的数据。

#### 10.5 多表连接

注意：连接 n 个表，至少需要 n-1 个连接条件。例如：连接三个表，至少需要两个连接条件

```
sql = """
            select v.video_id,v.insert_time,r.movie_id,c.collection_id from contents_dwd.dwd_con_video_info_enable v
            left join contents_dim.dim_con_video_movie_relation_2 r
            on v.video_id = r.video_id
            left join contents_dwd.dwd_content_video_collection_update c 
            on (v.video_id = c.video_id and c.opdate="{op}")
            where v.opdate="{op}" and manual_tag="Korea"
        """.format(op=target_date)
```

优化：当对 3 个或者更多表进行 join 连接时，如果每个 on 子句都使用相同的连接键的话，那么只会产生一个 MapReduce job。

#### 10.6 left semi join

left semi join是以左表为准，在右表中查找匹配的记录，如果查找成功，则仅返回左边的记录，否则返回null

#### 10.7 left anti join

left anti join与left semi join相反，是以左表为准，在右表中查找匹配的记录，如果查找成功，则返回null，否则仅返回左边的记录

### 11、排序

#### 11.1 order by

根据两个列进行排序：先name字段升序，再age字段降序

```
SELECT * FROM person ORDER BY name ASC, age DESC;
```

#### 11.2 sort by

Sort By：对于大规模的数据集 order by 的效率非常低。在很多情况下，并不需要全局排序，此时可以使用 sort by。

Sort by 为每个 reducer 产生一个排序文件。每个 Reducer 内部进行排序，对全局结果集来说不是排序。

```
# 使用 'REPARTITION' 提示按 'zip_code' 对数据进行分区
SELECT /*+ REPARTITION(zip_code) */ name, age, zip_code FROM person SORT BY name
+--------+----+--------+
|    name| age|zip_code|
+--------+----+--------+
|  Anil K|  27|   94588|
|  Dan Li|  18|   94588|
|  John V|null|   94588|
| Zen Hui|  50|   94588|
|Aryan B.|  18|   94511|
| David K|  42|   94511|
|Lalit B.|null|   94511|
+--------+----+--------+
```



### 12、cluster by

CLUSTER BY 子句用于首先根据输入表达式重新划分数据，然后对每个分区中的数据进行排序。这在语义上相当于执行 DISTRIBUTE BY 后跟 SORT BY。此子句只确保结果行在每个分区内排序，而且排序规则只能是升序。不保证输出的总顺序。

```
# 18\25在一个分区；16在一个分区。所以在分区1中18-25的顺序。分区2中只有16
SELECT age, name FROM person CLUSTER BY age;
+---+-------+
|age|   name|
+---+-------+
| 18| John A|
| 18| Anil B|
| 25|Zen Hui|
| 25| Mike A|
| 16|Shone S|
| 16| Jack N|
+---+-------+
```

### 13、DISTRIBUTE BY

DISTRIBUTE BY 子句用于根据输入表达式重新划分数据。与 CLUSTER BY 子句不同，这不会对每个分区内的数据进行排序。

distribute by 的分区规则是根据分区字段的 hash 码与 reduce 的个数进行模除后，余数相同的分到一个区。 Hive 要求 DISTRIBUTE BY 语句要写在 SORT BY 语句之前。

```
- 生成按年龄聚类的行。 年龄相同的人聚集在一起。
-- 与`CLUSTER BY` 子句不同，行不在分区内排序。
SELECT age, name FROM person DISTRIBUTE BY age;
+---+-------+
|age|   name|
+---+-------+
| 25|Zen Hui|
| 25| Mike A|
| 18| John A|
| 18| Anil B|
| 16|Shone S|
| 16| Jack N|
+---+-------+
```

### 14、case

CASE 子句使用规则根据指定的条件返回特定的结果，类似于其他编程语言中的 if/else 语句。

```
SELECT id, CASE id 
	WHEN 100 then 'bigger' 
	WHEN  id > 300 THEN '300'
    ELSE 'small' 
    END  
    as type
		FROM person;
+---+------+
| id|  type|
+---+------+
|100|bigger|
|200| small|
|300| small|
|400| small|
+---+------+	
```

### 15、PIVOT

Spark SQL 的 PIVOT 子句用于数据透视。我们可以根据特定的列值获得聚合值，这些值将转换为 SELECT 子句中使用的多个列。PIVOT 子句可以在表名或子查询之后指定。

为了防止OOM的情况，spark对pivot的数据量进行了限制，其可以通过spark.sql.pivotMaxValues 来进行修改，默认值为10000，这里是指piovt后的列数。

Spark SQL 的 PIVOT 结构为：

```
PIVOT ( { aggregate_expression [ AS aggregate_expression_alias ] } [ , ... ]
    FOR column_list IN ( expression_list ) )
```

参数：

- aggregate_expression：指定聚合表达式 (SUM(a), COUNT(DISTINCT b) 等)
- aggregate_expression_alias：指定聚合表达式的别名
- column_list：包含 FROM 子句中的列，该子句指定要用新列替换的列。我们可以使用括号来包围，例如（c1，c2）
- expression_list：指定用于匹配 column_list 中的值的新列作为聚合条件。我们还可以为它们添加别名

pivot( 聚合列 for 待转换列 in (列值) )   

```
 +----+----+----+
    |科目|姓名|分数|
    +----+----+----+
    |数学|张三|  88|
    |数学|李雷|  67|
    |数学|宫九|  77|
    |数学|王五|  65|
    |英语|张三|  77|
    |英语|宫九|  90|
    |英语|李雷|  24|
    |英语|王五|  90|
    |语文|李雷|  33|
    |语文|宫九|  87|
    |语文|张三|  92|
    |语文|王五|  87|
    +----+----+----+
    
 sql_content = '''select * from scores 
                 pivot
                 (
                     sum(`分数`) for
                     `姓名` in ('张三','王五','李雷','宫九')
                 )          
              '''

df_pivot = spark.sql(sql_content)
df_pivot.show()   

+----+----+----+----+----+
|科目|张三|王五|李雷|宫九|
+----+----+----+----+----+
|数学|  88|  65|  67|  77|
|英语|  77|  90|  24|  90|
|语文|  92|  87|  33|  87|
+----+----+----+----+----+
```

### 16、EXPLODE 一行变多行

EXPLODE(col)：将 hive 一列中复杂的 Array 或者 Map 结构拆分成多行。

friends 字段的值 ["bingbing","timor"]

```
select explode(friends) from test
```

配合lateral view 使用

### 17、LATERAL VIEW

用法：LATERAL VIEW udtf(expression) tableAlias AS columnAlias

解释：用于和 split, explode 等 UDTF 一起使用，它能够将一列数据拆成多行数据，在此基础上可以对拆分后的数据进行聚合。

数据准备

```
movie                     category
疑犯追踪                   悬疑、动作、科幻、剧情
lit to me                 悬疑、警匪、动作、心理、剧情
火影                       动作、战争、灾难
```

将电影分类中的数组数据展开结果如下

```
《疑犯追踪》 悬疑
《疑犯追踪》 动作
《疑犯追踪》 科幻
《疑犯追踪》 剧情
《Lie to me》 悬疑
《Lie to me》 警匪
《Lie to me》 动作
《Lie to me》 心理
《Lie to me》 剧情
《火影》 战争
《火影》 动作
《火影》 灾难
```

代码如下：

```
select movie,category_name
from movie_info
lateral VIEW
    explode(split(category,",")) movie_info_tmp as category_name
```

movie_info_tmp ：侧写表别名。侧写表使得炸列后的字段和原表保留关联。侧写表是对原表，即movie_info表进行侧写，需要放在from movie_info 之后。

category_name ：炸出来的字段的别名。

如果需要原表字段+炸裂后的数据，就需要侧写。如果不需要原表数据，则直接：

```
select explode(split(category,",")) from movie_info
```

### 18、concat

1、CONCAT(string A/col, string B/col…)：返回输入字符串连接后的结果，支持任意个输入字符串

```
select concat(money,'元')from table1;
```

2、CONCAT_WS(separator, str1, str2,...)：它是一个特殊形式的 CONCAT()。第一个参数剩余参数间的分隔符。分隔符可以是与剩余参数一样的字符串。如果分隔符是 NULL，返回值也将为 NULL。这个函数会跳过分隔符参数后的任何 NULL 和空字符串。分隔符将被加到被连接的字符串之间;

既然有concat函数为什么又有concat_ws呢？其实concat_ws是针对concat中一种特殊情形引入的，就是如果想要输出的字段用相同的字符进行分隔，应用concat函数就显得很笨重，你需要这样concat(s1,sep_str,s2,sep_str,s3.......)，这样的sql显得臃肿重复，这个时候你就需要用concat_ws函数了。concat_ws使用场景：输出字段用相同字符分隔的情形。

```
select concat_ws('|',name,age,sex,grade,telno) from table2;
```

3、COLLECT_SET(col)：函数只接受基本数据类型，它的主要作用是将某字段的值进行去重汇总，产生 Array 类型字段。

Hive中collect相关的函数有collect_list和collect_set。它们都是将分组中的某列转为一个数组返回，不同的是collect_list不去重而collect_set去重。

```
select username, collect_list(video_name) from t_visit_video group by username;
观看过视频列表有重复的，所以应该增加去重，使用collect_set
select username, collect_set(video_name) from t_visit_video group by username;
```



### 19、内联表 VALUES

```
-- 具有表别名的三行
select * from
 values ("one",1),("two",2),("three",null)
 as data(a,b);
+-----+----+
|    a|   b|
+-----+----+
|  one|   1|
|  two|   2|
|three|null|
+-----+----+
```



## 三、窗口函数

### 1 OVER(）

指定分析函数工作的数据窗口大小，这个数据窗口大小可能会随着行的变而变化。

需要原始数据，又需要聚合之后的结果，使用窗口函数

为什么over可以呢，其和groupby 有本质区别。窗口函数，每一条数据有自己的一个窗口。

窗函数的结构为：

```
window_function OVER
( [  { PARTITION | DISTRIBUTE } BY partition_col_name = partition_col_val ( [ , ... ] ) ]
  { ORDER | SORT } BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ]
  [ window_frame ] )
```

参数：

- window_function（窗口函数）
  - Ranking Functions：排序函数 `RANK | DENSE_RANK | PERCENT_RANK | NTILE | ROW_NUMBER`
  - Analytic Functions：解析函数 `CUME_DIST | LAG | LEAD`
  - Aggregate Functions：聚合函数 `MAX | MIN | COUNT | SUM | AVG | ...`

有关 Spark 聚合函数的完整列表，请参阅内置聚合函数文档。

- window_frame（窗框）：指定窗口的起始行和结束位置
  - 语法：`{ RANGE | ROWS } { frame_start | BETWEEN frame_start AND frame_end }`
  - frame_start 和 frame_end 具有以下语法：`UNBOUNDED PRECEDING | offset PRECEDING | CURRENT ROW | offset FOLLOWING | UNBOUNDED FOLLOWING`
  - 其中 offset 是指定相对于当前行位置的偏移。如果省略 frame_end，则默认为当前行。

参数

```
CURRENT ROW：当前行
n PRECEDING：往前 n 行数据
n FOLLOWING：往后 n 行数据
UNBOUNDED：起点，
UNBOUNDED PRECEDING 表示从前面的起点，
UNBOUNDED FOLLOWING 表示到后面的终点
LAG(col,n,default_val)：往前第 n 行数据
LEAD(col,n, default_val)：往后第 n 行数据
NTILE(n)：把有序窗口的行分发到指定数据的组中，各个组有编号，编号从 1 开始，对
于每一行，NTILE 返回此行所属的组的编号。注意：n 必须为 int 类型。
partition by 根据字段分区  + order by 排序
partition by 字段，限定开窗的最大范围。

distribute by  + sort by  是另一组。
```

### 2 实例

```
name，orderdate，cost
jack,2017-01-01,10
tony,2017-01-02,15
jack,2017-02-03,23
tony,2017-01-04,29
jack,2017-01-05,46
jack,2017-04-06,42
tony,2017-01-07,50
jack,2017-01-08,55
mart,2017-04-08,62
mart,2017-04-09,68
neil,2017-05-10,12
mart,2017-04-11,75
neil,2017-06-12,80
mart,2017-04-13,94
```

**需求**

（1）查询在 2017 年 4 月份购买过的顾客及总人数

（2）查询顾客的购买明细及月购买总额

（3）上述的场景, 将每个顾客的 cost 按照日期进行累加

（4）查询每个顾客上次的购买时间

（5）查询前 20%时间的订单信息



#### (1)查询在 2017 年 4 月份购买过的顾客及总人数

```
select name,count(*) over () 
from business 
where substring(orderdate,1,7) = '2017-04' 
group by name
```

over()表示窗口里没有东西。而 count是在窗口内部进行count，此时表示count所有行。

over()也是分组，和group by区别：group by 多对一。 over 一一对应。

group by更强调的是一个整体，就是组，只能显示一个组里满足聚合函数的一条记录。

over 在整体后更强调个体，能显示组里所有个体的记录

#### (2)查询顾客的购买明细及月购买总额

购买明细只做查询。

月购买总额，每个顾客，在每个月内进行sum。

```
select name,orderdate,cost,sum(cost) 
over(partition by month(orderdate))
from business
```

![](image\over1.png)

#### (3) 将每个顾客的 cost 按照日期进行累加

![](image\over2.png)

```
select 
    name,orderdate,
    cost,
    sum(cost) over(partition by name order by orderdate 
    rows between UNBOUNDED PRECEDING and current row)
from business;    
```

UNBOUNDED PRECEDING 表示从前面的起点

CURRENT ROW：当前行

即：由起点到当前行的聚合。

rows 必须跟在 order by 子句之后，对排序的结果进行限制，使用固定的行数来限制分区中的数据行数量。

ps:开窗的时候，如果涉及到了排序，两条相同的数据的窗口是一样的。这会造成意外事件。

#### (4)查看顾客上次的购买时间

即把时间字段下移一行，作为新的一列。或者说，当前行，获取上一行的orderdate字段，没有则为null。

```
select 
    name,orderdate,
    lag(orderdate,1,默认值，不给则为null)
    over(partition by name order by orderdate)
```

LAG(col,n,default_val)：往前第 n 行数据，没有则赋值默认值 default_val，如果没有给定，则为null

ps: 日志分析中，单跳转化率。上一个网页到下一个网页，涉及两条日志，进行拼接的时候，就需要lag。

#### (5)查询前 20%时间的订单信息

分五个组，取编号=1的组即为前20%。

NTILE(n)：把有序窗口的行分发到指定数据的组中，各个组有编号，编号从 1 开始，对于每一行，NTILE 返回此行所属的组的编号。注意：n 必须为 int 类型。

n 代表分几个组。

```
select 
    name,
    orderdate,
    cost,
    ntile(5) 
    over(order by orderdate) groupId
from business;    t1

select 
    name,
    orderdate,
    cost
from t1
where groupId=1; 
```

### 2 rank()

rank()：分数相同，不去掉数据(并排，假设有2、3分数相同，都排第二名，下面一个就是第四名，没有第三)  12245

dense_rank()：dense_rank()和rank over()很像，但学生成绩并列后并不会空出并列所占的名次 12234

ROW_NUMBER() ：不并排，假设二三分数相同，依然会排出第二名、第三名  12345

使用rank over()的时候，空值是最大的，如果排序字段为null, 可能造成null字段排在最前面，影响排序结果

```
select name,
subject,
score,
rank() over(partition by subject order by score desc) rp,
dense_rank() over(partition by subject order by score desc) drp,
row_number() over(partition by subject order by score desc) rmp
from score;
```

