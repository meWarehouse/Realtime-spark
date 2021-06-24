# Realtime-spark
spark streaming 数据分析平台
## 概览

《Titan数据运营系统》，本项目所适用的行业或业务背景有： 主营业务在线上(app/网站)进行的公司！ 这类公司，都需要针对用户的线上访问行为、消费行为、业务操作行为进行统计分析，数据挖掘！以 支撑公司的业务运营、精准画像营销、个性化推荐等，来提高业务转化率，改善公司运营效果！ 这些需求，都需要通过构建一个综合数据处理系统来支撑！

本项目是一个全栈闭环系统，我们先用flume-kafaka-flume对埋点日志服务器中日志进行读取，然后将日志放到我们的hdfs，然后在hive设计数仓，编写spark代码进行数仓表之间的转化以及ads层表到mysql的迁移，之后使用azkaban进行定时任务的调度，最后我们将mysql存的报表数据以web的形式展现出来。使用技术：Java/Scala语言，Hadoop、Spark、Hive、Kafka、Flume、Azkaban、SpringBoot，Bootstrap， Echart等。

项目的web展示：http://www.superhao.top:12121/src/material/installation.html **启动很慢，而且因为数据的原因会报错，建议到本文档底部去看web界面的展示**

**请注意：1. 我们所拥有的数据从2020.5.25-2020.7.20，但是我们网页一进去他默认日期会选择今天前七天的日期---今天的日期（比如8.01-8.08），所以一定会报错，请手动选择日期在5.25-7.20之间**

**2. 我们的网站部署到阿里云的学生机，所以会很慢，请耐心等待，谢谢**

博客地址：https://zihao.blog.csdn.net/category_10041667.html
