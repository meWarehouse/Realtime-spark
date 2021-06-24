package com.at.mall.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.at.mall.realtime.bean.dim.{ProvinceInfo, UserState}
import com.at.mall.realtime.bean.{OrderDetail, OrderInfo}
import com.at.mall.realtime.util.{ESUtil, MyKafkaSink, MykafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.hadoop.hbase.util.CollectionUtils

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author zero
 * @create 2021-04-06 20:25
 */
object OrderInfoApp {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("order_info_app").setMaster("local[4]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val topic = "ODS_ORDER_INFO"
    val groupId = "order_info_group"


    var recordInputDS: InputDStream[ConsumerRecord[String, String]] = null
    //获取偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      recordInputDS = MykafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      recordInputDS = MykafkaUtil.getKafkaStream(topic, ssc, groupId)
    }


    //获取数据读取范围
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordInputDStream: DStream[ConsumerRecord[String, String]] = recordInputDS.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })


    //基本的结构转换 ，补时间字段
    val orderInfoDS: DStream[OrderInfo] = recordInputDStream.map(record => {
      val str: String = record.value()

      var orderInfo: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])

      var timeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = timeArr(0)

      orderInfo.create_hour = timeArr(1).split(":")(0)

      orderInfo

    })




    // =================   首单   =====================

    val orderInfoWithFirstFlagDstream: DStream[OrderInfo] = orderInfoDS.mapPartitions { orderInfoIter =>
      val orderInfoList: List[OrderInfo] = orderInfoIter.toList

        if (orderInfoList != null && orderInfoList.size > 0) {

        val userIdList: List[Long] = orderInfoList.map(_.user_id)

        val sql = "select user_id , if_consumed from user_state2021 where user_id in ('" + userIdList.mkString("','") + "')"

        val jSONObjects: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userStateMap: Map[String, String] = jSONObjects.map(json => (json.getString("USER_ID"), json.getString("IF_CONSUMED"))).toMap


        for (elem <- orderInfoList) {

          val if_consum: String = userStateMap.getOrElse(elem.user_id + "", null)

          if (if_consum != null && "1" == if_consum) { //如果是消费用户  首单标志置为0
            elem.if_first_order = "0"
          } else {
            elem.if_first_order = "1"
          }
        }

      }

      orderInfoList.toIterator

    }





    // 利用hbase  进行查询过滤 识别首单，只能进行跨批次的判断
    //  如果新用户在同一批次 多次下单 会造成 该批次该用户所有订单都识别为首单
    //  应该同一批次一个用户只有最早的订单 为首单 其他的单据为非首单
    // 处理办法： 1 同一批次 同一用户  2 最早的订单  3 标记首单
    //           1 分组： 按用户      2  排序  取最早  3 如果最早的订单被标记为首单，除最早的单据一律改为非首单
    //           1  groupbykey       2  sortWith    3  if ...、
    val orderInfoGroupbyDS: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithFirstFlagDstream.map(orderinfo => (orderinfo.user_id, orderinfo)).groupByKey()

    val orderInfoWithFirstDS: DStream[OrderInfo] = orderInfoGroupbyDS.flatMap {
      case (userid, orderInfoIter) =>
        if (orderInfoIter.size > 1) {
          val toList: List[OrderInfo] = orderInfoIter.toList

          val orderInfoesSort: List[OrderInfo] = toList.sortWith((order1, order2) => order1.create_time < order2.create_time)
          val firstOrder: OrderInfo = orderInfoesSort(0)
          if (firstOrder.if_first_order == "1") {
            for (i <- 1 until orderInfoesSort.size) {
              orderInfoesSort(i).if_first_order = "0"
            }
          }
          orderInfoesSort
        } else orderInfoIter.toList
    }



    //====================================================关联地区
    // 优化 ： 因为传输量小  使用数据的占比大  可以考虑使用广播变量     查询hbase的次数会变小   分区越多效果越明显
    //利用driver进行查询 再利用广播变量进行分发
    val orderInfoWithProvinceDstream: DStream[OrderInfo] = orderInfoWithFirstDS.transform { rdd =>

      var sql = "select id,name,area_code,iso_code,iso_3166_2 from mall2021_province_info"
      val provinceInfoJson: List[JSONObject] = PhoenixUtil.queryList(sql)

      //封装广播变量
      val provinceMap: Map[String, ProvinceInfo] = provinceInfoJson.map { elem =>
        val pInfo: ProvinceInfo = com.at.mall.realtime.bean.dim.ProvinceInfo(
          elem.getString("ID"),
          elem.getString("NAME"),
          elem.getString("AREA_CODE"),
          elem.getString("ISO_CODE"),
          elem.getString("ISO_3166_2")
        )

        (pInfo.id, pInfo)
      }.toMap
      val privinceBC: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)

      val orderInfoWithProvRDD: RDD[OrderInfo] = rdd.map { orderEle =>
        val provMap: Map[String, ProvinceInfo] = privinceBC.value
        val info: ProvinceInfo = provMap.getOrElse(orderEle.province_id.toString, null)
        if (info != null) {
          orderEle.province_name = info.name
          orderEle.province_area_code = info.area_code
          orderEle.province_iso_code = info.iso_code
          orderEle.province_iso_3166_2 = info.iso_3166_2
        }
        orderEle
      }
      orderInfoWithProvRDD
    }



    //////////////////用户信息关联//////////////////////////
    val orderInfoWithUserDstream: DStream[OrderInfo] = orderInfoWithProvinceDstream.mapPartitions { orderInfoIter =>
      val orderInfoList: List[OrderInfo] = orderInfoIter.toList

      if (orderInfoList != null && orderInfoList.size > 0) {
        val userIds: List[Long] = orderInfoList.map(_.user_id)

        var sql: String = "select id,age_group,gender_name from  mall2021_user_info where id in('" + userIds.mkString("','") + "')"

        val jSONObjects: List[JSONObject] = PhoenixUtil.queryList(sql)
        if (jSONObjects != null && jSONObjects.size > 0) {
          val userJsonMap: Map[String, JSONObject] = jSONObjects.map(json => (json.getString("ID"), json)).toMap

          for (elem <- orderInfoList) {
            val nObject: JSONObject = userJsonMap.getOrElse(elem.user_id.toString, null)
            if (nObject != null) {
              elem.user_gender = nObject.getString("GENDER_NAME")
              elem.user_age_group = nObject.getString("AGE_GROUP")
            }
          }
        }
      }
      orderInfoList.toIterator
    }


    orderInfoWithUserDstream.print(1000)

    orderInfoWithUserDstream.cache()

    // 保存 用户状态--> 更新hbase 维护状态
    //保存首单用户
    orderInfoWithUserDstream.foreachRDD { rdd =>
      println(rdd)
      val newCoutomed: RDD[UserState] = rdd.filter(_.if_first_order == "1").map(orderinfo => UserState(orderinfo.user_id.toString, orderinfo.if_first_order))
      newCoutomed.saveToPhoenix("USER_STATE2021", Seq("USER_ID", "IF_CONSUMED"), new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181"))
    }

     //将数据保存到es
    orderInfoWithUserDstream.foreachRDD { rdd =>

      rdd.foreachPartition { orderinfoIter =>

        val orderinfoList: List[OrderInfo] = orderinfoIter.toList
        if (orderinfoList != null && orderinfoList.size > 0) {

          val tuples: List[(String, OrderInfo)] = orderinfoList.map { info => (info.id.toString, info) }
          val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
          println("---------------------------------")
//          ESUtil.bulkDoc(tuples, "mall2021_order_info" + dateStr)

          for (orderinfo <- orderinfoList) {
            val str: String = JSON.toJSONString(orderinfo, new SerializeConfig(true))

            MyKafkaSink.send("DWD_ORDER_INFO",orderinfo.id.toString, str)
          }
        }
      }
      OffsetManager.saveOffset(topic, groupId, offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()




  }
}
