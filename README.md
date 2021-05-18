### 

## ![rocksredis logo](https://images.gitee.com/uploads/images/2021/0506/141100_1d885747_1205442.png "image-20210506131823794.png")

##### 一、介绍

​	rocksredis 是redis内存数据库的增强版，采用rocksdb嵌入式数据库引擎作为redis的持久化数据管理。

​	redis 的高性能已经成为缓存数据管理的首选，  redis的持久化有两种方式RDB和AOF一个是周期性持久化、一个是以日志模式写入 ，随着文件的增大redis的读写性能会降低，因此，redis只能作为一定数据量的缓存数据库管理。

​	rocksdb 为快速而又低延迟的存储设备（例如闪存或者高速硬盘）而特殊优化处理。 RocksDB将最大限度的发挥闪存和RAM的高度率读写性能。rocksdb在批量batchwrite处理上性能优越，因此需要在rocksredis‘冷热’数据分离的基础上增加数据的缓存机制以保证rocksdb的batchwrite处理。

​    rocksredis 采用magnet作为中间件实现磁性缓存和持久层的连接和管理，数据流先通过redis的缓存和管理下来，在通过magnet的数据处理线程池将缓存数据批量写入rocksdb.

rocksredis ,可以支持10万以上个keydb,每个keydb可以存储1亿条以上数据，单机版的rocksredis可以支持10万亿以上的数据存储.

应用场景，rocksredis非常适合以下场景：

1. 物联网、工业互联网的设备及终端的采集数据存储
2. 私有云的备份数据、人工智能需要的训练数据存储
3. 由于资源限制无法部署和应用，例如hadoop、hbase等分布式集群作为数据存储的场景，可以有rocksredis替代
4. 缓存数据和持久化数据使用交替频繁的场景
5. 数据量大写入频率高的应用场景



##### 二、架构

###### ![rocksredis 架构](https://images.gitee.com/uploads/images/2021/0506/141215_92a7fc5d_1205442.png "image-20210506133937752.png")



##### 三、安装

> 目录
>

```
|--libs
|--bin
	|--redis.conf
	|--runstart.sh
		
libs 目录为rocksredis运行依赖库
bin  目录为rocksredis执行程序
redis.conf  配置文件
runstart.sh 执行脚本
```

> redis.conf  配置说明
>

```
########################### ROCKSREDIS CONFIG #######################
#Select a DB of redis as the pipeline of rocksreids db
#If redis is a cluster, DB is 0 by default
#If it is single, you can choose from 0 to 15
#If this option is commented out, the rocksredis database will be closed, 
#and rocksredis will be used as the cache database.
rocksredis_db_select 15

#Rockredis Data storage path
#Default path is /data
rocksredis_db_dir  /home/data

#Batch data write length size,It can be adjusted according to the write frequency. 
#The default size is 2000
rocks_write_batch_size  2000

#Set the size of a single memtable. Once the memtable exceeds this size, 
#it will be marked as immutable and a new one will be created.
#The default size is 64MB  ,64*1024*1024
write_buffer_size 67108864

#Set the maximum number of memtables, active and non modifiable. 
#If the active memtable is filled, then the total number of memtables is greater than max_ write_ buffer_ Number, 
#we will stall subsequent writes. Occurs when the drop process is slower than the write speed.
#The default size is 3
max_write_buffer_number 3

#Is the number of disk dropping concurrent. Usually, a setting of 1 is enough
#Here, the default size is 4
max_background_flushes 4

#Is the maximum number of threads for background compression. 
#The default is 1, but in order to make full use of CPU and storage, 
#you may want to increase the number of cores close to the system. 
#The default size here is 8
max_background_compactions 8

#Background execution thread of data batch disk dropping processing
#The default value is 10, which can be configured according to the actual needs
batch_write_background_threadpools 10

```

```
#选择redis的一个db作为rocksdb的持久化管道，单机版的redis默认db0~db15,缺省状态下为15；
#其余 db可以作为redis的缓存正常使用；弱不希望rocksdb作持久化则可以设置rocksredis_db_select为-1；
rocksredis_db_select 15 

#rocksredis 持久化数据库的路径设置
rocksredis_db_dir  /home/data

#rocksredis 批量持久化的batchsize 
rocks_write_batch_size  2000

#rocksdb 的写缓存buffer size
write_buffer_size 67108864

#rocksdb 的写缓存buffer 数量
max_write_buffer_number 3

#rocksdb 后台刷新线程数
max_background_flushes 4

#rocksdb 后台合并线程数
max_background_compactions 8

#rocksdb 后台批量持久化的线程池数
batch_write_background_threadpools 10
```

> 运行

```
执行运行脚本 ./runstart.sh
6.0.8 为redis的基础版本
06 为rocksredis的版本
```

![运行](https://images.gitee.com/uploads/images/2021/0506/141251_64f8e766_1205442.png "image-20210506113254448.png")

------



##### 四、使用

Rocksredis 完全兼容redis的驱动和操作方式，rocksredis支持的操作指令，

| num  | command                                                      |
| ---- | ------------------------------------------------------------ |
| 1    | [HDEL key field1 field2] 删除一个或多个哈希表字段            |
| 2    | HEXISTS key field 查看哈希表 key 中，指定的字段是否存在。    |
| 3    | HGET key field 获取存储在哈希表中指定字段的值。              |
| 4    | HGETALL key 获取在哈希表中指定 key 的所有字段和值            |
| 5    | HKEYS key 获取所有哈希表中的字段                             |
| 6    | HLEN key 获取哈希表中字段的数量                              |
| 7    | [HMGET key field1 field2] 获取所有给定字段的值               |
| 8    | [HMSET key field1 value1 field2 value2 ] 同时将多个 field-value (域-值)对设置到哈希表 key 中。 |
| 9    | HSET key field value 将哈希表 key 中的字段 field 的值设为 value 。 |
| 10   | HSETNX key field value 只有在字段 field 不存在时，设置哈希表字段的值。 |
| 11   | HVALS key 获取哈希表中所有值。                               |
| 12   | HSCAN key cursor [MATCH pattern] [COUNT count] 迭代哈希表中的键值对。 |
| 13   | DEL 命令用于删除已存在的键                                   |
| 14   | Strlen 命令用于获取指定 key 所储存的字符串值的长度           |
| 15   | Scan 命令用于迭代数据库中的数据库键                          |
| 16   | Type 命令用于返回 key 所储存的值的类型                       |
| 17   | Sadd 命令将一个或多个成员元素加入到集合中，已经存在于集合的成员元素将被忽略 |
| 18   | Scard 命令返回集合中元素的数量                               |
| 19   | Sismember 命令判断成员元素是否是集合的成员                   |
| 20   | Smembers 命令返回集合中的所有的成员。 不存在的集合 key 被视为空集合 |
| 21   | Spop 命令用于移除集合中的指定 key 的一个或多个随机元素，移除后会返回移除的元素 |
| 22   | Sscan 命令用于迭代集合中键的元素                             |
| 23   | Srem 命令用于移除集合中的一个或多个成员元素                  |
| 24   | SET 命令用于设置给定 key 的值。如果 key 已经存储其他值， SET 就覆写旧值，且无视类型 |
| 25   | Get 命令用于获取指定 key 的值。如果 key 不存在，返回 nil 。如果key 储存的值不是字符串类型，返回一个错误 |

> JAVA

```
引入redis驱动到pom
<!-- redis -->
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>2.9.0</version>
</dependency>
```

> PYTHON

```
pip install redis==2.10.6
```

> C/C++

```
引入库hiredis.a
```



##### 五、性能评测

> ###### cpu:   2核
>
> ###### 内存：8G
>
> ###### 二手机械磁盘：500G，实际检测与硬盘性能有关

###### 1、写性能测试

测试一、Hash 存储，hsetnx()

```
Items:10000,Data:1Kbyte,Type:String
start:Mon Apr 26 13:36:21 CST 2021
耗时: 28.324 (s)
```

### ![输入图片说明](https://images.gitee.com/uploads/images/2021/0506/141318_44b089e6_1205442.png "image-20210426133705788.png")

```
Items:50000,Data:1Kbyte,Type:String
start:Mon Apr 26 13:36:21 CST 2021
耗时: 65.102s) 
```

### ![输入图片说明](https://images.gitee.com/uploads/images/2021/0506/141332_e4f8f5e3_1205442.png "image-20210426150322994.png")

```
Items:100000,Data:1Kbyte,Type:String
start:Mon Apr 26 15:09:03 CST 2021
耗时:172.914 (s)
```

### ![输入图片说明](https://images.gitee.com/uploads/images/2021/0506/141346_3c9213ee_1205442.png "image-20210426151408310.png")

测试二、Hash 存储，hsetnx()

```
Items:10000,Data:10Kbyte,Type:String
start:Mon Apr 26 15:26:14 CST 2021
耗时: 27.288(s)

```

### ![输入图片说明](https://images.gitee.com/uploads/images/2021/0506/141400_99e02591_1205442.png "image-20210426152748437.png")

```
Items:50000,Data:10Kbyte,Type:String
start:Mon Apr 26 16:19:51 CST 2021
耗时: 137.374(s)
```

### ![输入图片说明](https://images.gitee.com/uploads/images/2021/0506/141413_8cb74d01_1205442.png "image-20210426162317243.png")

```
Items:100000,Data:10Kbyte,Type:String
start:Mon Apr 26 16:23:47 CST 2021
耗时: 275.583(s)
```

### ![输入图片说明](https://images.gitee.com/uploads/images/2021/0506/141426_0d7087a5_1205442.png "image-20210426163543639.png")

###### 2、读性能测试 Hash ，hscan()

```
Read Items:10000,Data:10KB,Type:String
start:Mon Apr 26 16:42:41 CST 2021
耗时: 8.567(s)
```

```
Read Items:100000，Data:10KB,Type:String
start:Mon Apr 26 16:43:32 CST 2021
耗时: 85.449(s)
```

### ![输入图片说明](https://images.gitee.com/uploads/images/2021/0506/141442_742a2445_1205442.png "image-20210426164614830.png")



##### 六、下载

gitee

- [centos7 64bit server](https://gitee.com/RocksCloud/rocksredis_centos7_x86_64)



github

- [centos7 64bit server](https://github.com/RocksCloud/rocksredis_centos7_x86_64)



其他

https://gitee.com/rockscloud

https://github.com/rockscloud



#### 七、技术交流群

目前，rocksredis 基础评测阶段已完成，目前已经在Rocksiot物联云平台、设备智能管理平台、预测性维护等平台上得到广泛应用，欢迎大家参与技术交流.

![输入图片说明](https://images.gitee.com/uploads/images/2021/0511/114648_5eee5a94_1205442.png "个人微信.png")






