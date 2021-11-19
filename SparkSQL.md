## 一、基础

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

进行聚合的字段必须出现在select语句中

```
# 计算 emp 表每个部门的平均工资
select t.deptno, avg(t.sal) avg_sal from emp t group by t.deptno;
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



