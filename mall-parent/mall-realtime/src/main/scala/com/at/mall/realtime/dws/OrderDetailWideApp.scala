package com.at.mall.realtime.dws

import com.alibaba.fastjson.JSON
import com.at.mall.realtime.bean.{OrderDetail, OrderInfo}
import com.at.mall.realtime.util.{MyKafkaSink, MykafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.serializer.SerializeConfig
import redis.clients.jedis.Jedis
import com.at.mall.realtime.bean._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import java.{lang, util}
import scala.collection.mutable.ListBuffer

/**
 * @author zero
 * @create 2021-04-07 21:18
 */
object OrderDetailWideApp {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("order_detail_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topicOrderInfo = "DWD_ORDER_INFO"
    val groupIdOrderInfo = "dws_order_info_group"
    val kafkaOffsetOrderinfo: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderInfo, groupIdOrderInfo)
    var recordInputStreamOrderInfo: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetOrderinfo != null && kafkaOffsetOrderinfo.size > 0) {
      recordInputStreamOrderInfo = MykafkaUtil.getKafkaStream(topicOrderInfo, ssc, kafkaOffsetOrderinfo, groupIdOrderInfo)
    } else {
      recordInputStreamOrderInfo = MykafkaUtil.getKafkaStream(topicOrderInfo, ssc, groupIdOrderInfo)
    }
    var offsetRangesOrderinfo: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordInputDSOrderinfo: DStream[ConsumerRecord[String, String]] = recordInputStreamOrderInfo.transform { rdd =>
      offsetRangesOrderinfo = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val topicOrderDetail = "DWD_ORDER_DETAIL"
    val groupIdOrderDetail = "dws_order_detail_group"
    val kafkaOffsetOrderDetail: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderDetail, groupIdOrderDetail)
    var recordInputStreamOrderDetail: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetOrderinfo != null && kafkaOffsetOrderinfo.size > 0) {
      recordInputStreamOrderDetail = MykafkaUtil.getKafkaStream(topicOrderDetail, ssc, kafkaOffsetOrderinfo, groupIdOrderDetail)
    } else {
      recordInputStreamOrderDetail = MykafkaUtil.getKafkaStream(topicOrderDetail, ssc, groupIdOrderDetail)
    }
    var offsetRangesOrderDetail: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordInputDSOrderDetail: DStream[ConsumerRecord[String, String]] = recordInputStreamOrderDetail.transform { rdd =>
      offsetRangesOrderDetail = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    ///////////////////////////////////
    /////////////结构调整//////////////
    val orderInfoDstream: DStream[OrderInfo] = recordInputDSOrderinfo.map { record =>
      val str: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
      orderInfo
    }
    val orderDetailDstream: DStream[OrderDetail] = recordInputDSOrderDetail.map { record =>
      val str: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(str, classOf[OrderDetail])
      orderDetail
    }

//
//        orderInfoDstream.print(1000)
//        orderDetailDstream.print(1000)

    //////////////////////////////////////////////
    ////合并两流
    //////////////////////////////////////////////
    val orderinfoMapKeyDS: DStream[(Long, OrderInfo)] = orderInfoDstream.map { orderinfo => (orderinfo.id, orderinfo) }
    val orderdetailMapKeyDS: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderdetail => (orderdetail.order_id, orderdetail))

    //    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderinfoMapKeyDS.join(orderdetailMapKeyDS)
    //
    //    orderJoinedDstream.print(1000)


    //1.开窗
    val orderInfoWithKeyWindowDstream: DStream[(Long, OrderInfo)] = orderinfoMapKeyDS.window(Seconds(10), Seconds(5))
    val orderDetailWithKeyWindowDstream: DStream[(Long, OrderDetail)] = orderdetailMapKeyDS.window(Seconds(10), Seconds(5))

    //2.join
    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyWindowDstream.join(orderDetailWithKeyWindowDstream)

    //3.去重
    val orderJoinedNewDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderJoinedDstream.mapPartitions { orderJoinedTupleItr =>
      val client: Jedis = RedisUtil.getJedisClient
      val orderJoinedNewList = new ListBuffer[(Long, (OrderInfo, OrderDetail))]()
      var key = "order_join_keys"
      for ((orderId, (orderinfo, orderdetail)) <- orderJoinedTupleItr) {
        val ifNew: lang.Long = client.sadd(key, orderdetail.id.toString)

        if (ifNew == 1L) {
          orderJoinedNewList.append((orderId, (orderinfo, orderdetail)))
        }
      }
      client.close()
      orderJoinedNewList.toIterator
    }


    val orderJoinedNew: DStream[OrderDetailWide] = orderJoinedNewDstream.map { case (orderid, (orderinfo, orderDetail)) => new OrderDetailWide(orderinfo, orderDetail) }

    orderJoinedNew.cache()
        orderJoinedNew.print(1000)


    ////////////////////////////////////////////
    ////缓存  com.at.mall.realtime.otherDoubleStreamJoin
    ////////////////////////////////////////////


    /////////////////////////////////////////////////////////
    // 计算实付分摊需求
    ////////////////////////////////////////
    // 思路 ：：
    //    每条明细已有        1  原始总金额（original_total_amount） （明细单价和各个数的汇总值）
    //    2  实付总金额 (final_total_amount)  原始金额-优惠金额+运费
    //    3  购买数量 （sku_num)
    //    4  单价      ( order_price)
    //
    //    求 每条明细的实付分摊金额（按明细消费金额比例拆分）
    //
    //    1  33.33   40    120
    //    2  33.33   40    120
    //    3   ？     40    120
    //
    //    如果 计算是该明细不是最后一笔
    //      使用乘除法      实付分摊金额/实付总金额= （数量*单价）/原始总金额
    //      调整移项可得  实付分摊金额=（数量*单价）*实付总金额 / 原始总金额
    //
    //    如果  计算时该明细是最后一笔
    //      使用减法          实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
    //    1 减法公式
    //      2 如何判断是最后一笔
    //      如果 该条明细 （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）
    //
    //
    //    两个合计值 如何处理
    //      在依次计算的过程中把  订单的已经计算完的明细的【实付分摊金额】的合计
    //    订单的已经计算完的明细的【数量*单价】的合计
    //    保存在redis中 key设计
    //    type ?   hash      key? order_split_amount:[order_id]  field split_amount_sum ,origin_amount_sum    value  ?  累积金额

    //  伪代码
    //    1  先从redis取 两个合计    【实付分摊金额】的合计，【数量*单价】的合计
    //    //    2 先判断是否是最后一笔  ： （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）
    //    //    3.1  如果不是最后一笔：
    //                      // 用乘除计算 ： 实付分摊金额=（数量*单价）*实付总金额 / 原始总金额
    //
    //    //    3.2 如果是最后一笔
    //                     // 使用减法 ：   实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
    //    //    4  进行合计保存
    //              //  hincr
    ////              【实付分摊金额】的合计，【数量*单价】的合计
    val orderWideWithSplitDstream: DStream[OrderDetailWide] = orderJoinedNew.mapPartitions { orderWithIter =>

      val jedis: Jedis = RedisUtil.getJedisClient
      //////////////1  先从redis取 两个合计    【实付分摊金额】的合计，【数量*单价】的合计
      val orderWideList: List[OrderDetailWide] = orderWithIter.toList
      for (orderWide <- orderWideList) {

        // type ?   hash      key? order_split_amount:[order_id]  field split_amount_sum ,origin_amount_sum    value  ?  累积金额
        val key: String = "order_split_amount" + orderWide.order_id
        val orderSumMap: util.Map[String, String] = jedis.hgetAll(key)

        var splitAmountSum: Double = 0D
        var originAmountSum: Double = 0D

        if (orderSumMap != null && orderSumMap.size() > 0) {
          //第一次切分是没有
          splitAmountSum = orderSumMap.get("split_amount_sum").toDouble
          originAmountSum = orderSumMap.get("origin_amount_sum").toDouble
        }

        /////////////2 先判断是否是最后一笔  ： （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）

        //单条明细的原始金额  数量*单价
        val detailOrginAmount: Double = orderWide.sku_num * orderWide.sku_price
        val restOriginAmount: Double = orderWide.final_total_amount - originAmountSum
        if (detailOrginAmount == restOriginAmount) {
          //3.1  最后一笔 用减法 ：实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
          orderWide.final_detail_amount = orderWide.final_total_amount - splitAmountSum
        } else {
          //3.2  不是最后一笔 用乘除  实付分摊金额=（数量*单价）*实付总金额 / 原始总金额
          orderWide.final_detail_amount = detailOrginAmount * orderWide.final_total_amount / orderWide.original_total_amount
          orderWide.final_detail_amount = Math.round(orderWide.final_detail_amount * 100D) / 100D
        }

        ////////////////4  进行合计保存
        splitAmountSum += orderWide.final_detail_amount
        originAmountSum += detailOrginAmount
        orderSumMap.put("split_amount_sum", splitAmountSum.toString)
        orderSumMap.put("origin_amount_sum", originAmountSum.toString)
        jedis.hmset(key, orderSumMap)

      }
      jedis.close()
      orderWideList.toIterator
    }


    orderWideWithSplitDstream.cache()
    orderWideWithSplitDstream.map(orderwide => JSON.toJSONString(orderwide, new SerializeConfig(true))).print(1000)



    val sparkSession: SparkSession = SparkSession.builder().appName("order_detail_wide_spark_app").getOrCreate()
    import sparkSession.implicits._

    val orderWideKafkaSentDstream: DStream[OrderDetailWide] = orderWideWithSplitDstream.mapPartitions { orderWideItr =>
      val list: List[OrderDetailWide] = orderWideItr.toList
      for (elem <- list) {
        MyKafkaSink.send("DWS_ORDER_WIDE", JSON.toJSONString(elem, new SerializeConfig(true)))
      }
      list.toIterator
    }

    //将数据写入clickhouse
    orderWideKafkaSentDstream.foreachRDD { rdd =>
      val df: DataFrame = rdd.toDF()

      df.write
        .mode(org.apache.spark.sql.SaveMode.Append)
        .option("batchsize", "100")
        .option("isolationLevel", "NONE")
        .option("numPartitions", "4")
        .option("isolationLevel", "NONE")
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://hadoop102:8123/testdb01", "order_wide", new Properties())


      OffsetManager.saveOffset(topicOrderInfo, groupIdOrderInfo, offsetRangesOrderinfo)
      OffsetManager.saveOffset(topicOrderDetail, groupIdOrderDetail, offsetRangesOrderDetail)
    }



    ssc.start()
    ssc.awaitTermination()


  }

}
