package com.at.mall.realtime.otherDoubleStreamJoin

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
import org.apache.hadoop.conf.Configuration

import java.{lang, util}
import scala.collection.mutable.ListBuffer

/**
 * @author zero
 * @create 2021-04-08 20:38
 */
object SaleApp {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("order_detail_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val topicOrderInfo = "DWD_ORDER_INFO"
    val groupIdOrderInfo = "dws_order_info_group"
    val kafkaOffsetOrderinfo: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderInfo, groupIdOrderInfo)
    var recordInputStreamOrderInfo: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetOrderinfo != null && kafkaOffsetOrderinfo.size > 0) {
      recordInputStreamOrderInfo = MykafkaUtil.getKafkaStream(topicOrderInfo, ssc, kafkaOffsetOrderinfo, groupIdOrderInfo)
    } else {
      recordInputStreamOrderInfo = MykafkaUtil.getKafkaStream(topicOrderInfo,ssc,groupIdOrderInfo)
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
      recordInputStreamOrderDetail = MykafkaUtil.getKafkaStream(topicOrderDetail,ssc,groupIdOrderDetail)
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
    orderInfoDstream.print(1000)
    orderDetailDstream.print(1000)

    //////////////////////////////////////////////
    ////合并两流
    //////////////////////////////////////////////
    val orderinfoMapKeyDS: DStream[(Long, OrderInfo)] = orderInfoDstream.map { orderinfo => (orderinfo.id, orderinfo) }
    val orderdetailMapKeyDS: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderdetail => (orderdetail.order_id, orderdetail))


    ////////////////////////////////////////////
    //1.开窗
    //2.join
    //3.去重
    ////////////////////////////////////////////
    //    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderinfoMapKeyDS.join(orderdetailMapKeyDS)
    //
    //    orderJoinedDstream.print(1000)


    ////////////////////////////////////////////
    ////缓存
    ////////////////////////////////////////////
    val orderFullJoinedDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderinfoMapKeyDS.fullOuterJoin(orderdetailMapKeyDS)


    orderFullJoinedDstream.map{case (userId,(orderinfoOption,orderDetailOption)) =>

      //1主表部分
      val client: Jedis = RedisUtil.getJedisClient

      val saleDetailList = new ListBuffer[SaleDetail]
      if(orderinfoOption != None){
        val orderInfo: OrderInfo = orderinfoOption.get

        //1.1 在同一批次能够关联， 两个对象组合成一个新的宽表对象
        if(orderDetailOption != None){
          val orderDetail: OrderDetail = orderDetailOption.get
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList+=saleDetail
        }

        //1.2  转换成json写入缓存
        val orderinfoJson: String = JSON.toJSONString(orderInfo, new SerializeConfig(true))
        // redis写入   type ? string        key ?    order_info:[order_id]        value ?   orderInfoJson   ex? 600s
        // 为什么不用集合 比如hash 来存储整个的orderInfo 清单呢
        //1 没必要 因为不需要一下取出整个清单
        //2 超大hash 不容易进行分布式
        // 3  hash 中的单独k-v  没法设定过期时间
        val orderInfokey = "order_info:" + orderInfo.id
        client.setex(orderInfokey,60,orderinfoJson)

        //1.3   查询缓存中是否有对应的orderDetail
        val orderDetailKey = "order_detail:" + orderInfo.id
        val orderDetailJsonSet: util.Set[String] = client.smembers(orderDetailKey)
        if(orderDetailJsonSet != null && orderDetailJsonSet.size() > 0){
          import scala.collection.JavaConversions._
          for (orderDetailJsonString <- orderDetailJsonSet) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonString, classOf[OrderDetail])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }

        }
      }else{
        val orderDetail: OrderDetail = orderDetailOption.get

        //2.1 转换成json写入缓存
        val orderDetailJson: String = JSON.toJSONString(orderDetail,new SerializeConfig(true))

       // Redis ？  type ?   set     key ?   order_detail:[order_id]       value ? orderDetailJsons
        //从表如何存储到redis?
        val orderDetailKey = "order_detail:" + orderDetail.order_id
        client.sadd(orderDetailKey, orderDetailJson)
        client.expire(orderDetailKey, 60)

        //2.2  从表查询缓存中主表信息
        val orderInfokey = "order_info:" + orderDetail.order_id
        val orderInfoJson: String = client.get(orderInfokey)
        if (orderInfoJson != null && orderInfoJson.length > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          saleDetailList += new SaleDetail(orderInfo, orderDetail)
        }

      }
      client.close()
      saleDetailList

    }





    ssc.start()
    ssc.awaitTermination()




  }






}
