# hadoop实例

创建于2021-10-18

代码基于：

- Hadoop 2.10.1
- Eclipse Photon
- hadoop-eclipse-plugin-2.8.3
- HBase 2.3.7



## 结构：

### MapReduce(mapr)

- 数据去重 `DuplicateMain`  

- 矩阵面积排序 `RectangleMain`

- 普通排序 `SortJobMain`

- 词频统计 `WordCountMain`

- 词频统计-按词频降序排序 `WordCountSortMain`

- 计算平均成绩 `MeanScoreMain`

- 未建机场的城市统计 `GansuAirportsMain`


### 工具包(tools)

- HDFS `HDFSTools`  

- HBase `HBaseTools`

- Sqoop `SqoopTools` 

- Hadoop序列化 `HadoopTools` 