package com.at.mall.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.at.mall.realtime.bean.OrderDetail
import com.at.mall.realtime.util.{MyKafkaSink, MykafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.serializer.SerializeConfig

/**
 * @author zero
 * @create 2021-04-07 20:47
 */
object OrderDetailApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("order_detail_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))


    val topic = "ODS_ORDER_DETAIL"
    val groupId = "order_detail_group"

    val kafkaOffset: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffset != null && kafkaOffset.size > 0) {
      recordInputStream = MykafkaUtil.getKafkaStream(topic, ssc, kafkaOffset, groupId)
    } else {
      recordInputStream = MykafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordInputDS: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    val orderDetailDstream: DStream[OrderDetail] = recordInputDS.map { record =>
      val str: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(str, classOf[OrderDetail])
      orderDetail
    }


    /*
      bin/maxwell-bootstrap --user maxwell  --password maxwell --host hadoop102  --database mall_ss_db --table base_category3  --client_id maxwell_1
      bin/maxwell-bootstrap --user maxwell  --password maxwell --host hadoop102  --database mall_ss_db --table user_info  --client_id maxwell_1
      bin/maxwell-bootstrap --user maxwell  --password maxwell --host hadoop102  --database mall_ss_db --table base_trademark  --client_id maxwell_1
      bin/maxwell-bootstrap --user maxwell  --password maxwell --host hadoop102  --database mall_ss_db --table spu_info  --client_id maxwell_1
      bin/maxwell-bootstrap --user maxwell  --password maxwell --host hadoop102  --database mall_ss_db --table sku_info  --client_id maxwell_1
     */

    // 合并维表数据
    // 品牌 分类 spu  作业
    //  orderDetailDstream.
    val orderDetailWithSkuDstream: DStream[OrderDetail] = orderDetailDstream.mapPartitions { orderDetailiter =>

      val orderDetailList: List[OrderDetail] = orderDetailiter.toList
      if (orderDetailList.size > 0) {
        val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
        val sql = "select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name  from mall2021_sku_info  where id in ('" + skuIdList.mkString("','") + "')"
        val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val skuJsonObjMap: Map[Long, JSONObject] = skuJsonObjList.map(skuJsonObj => (skuJsonObj.getLongValue("ID"), skuJsonObj)).toMap
        for (orderDetail <- orderDetailList) {
          val skuJsonObj: JSONObject = skuJsonObjMap.getOrElse(orderDetail.sku_id, null)
          orderDetail.spu_id = skuJsonObj.getLong("SPU_ID")
          orderDetail.spu_name = skuJsonObj.getString("SPU_NAME")
          orderDetail.tm_id = skuJsonObj.getLong("TM_ID")
          orderDetail.tm_name = skuJsonObj.getString("TM_NAME")
          orderDetail.category3_id = skuJsonObj.getLong("CATEGORY3_ID")
          orderDetail.category3_name = skuJsonObj.getString("CATEGORY3_NAME")
        }
      }
      orderDetailList.toIterator
    }


    orderDetailWithSkuDstream.print(1000)

    orderDetailWithSkuDstream.foreachRDD(rdd =>
      rdd.foreach{orderD =>
        val orderDstr: String = JSON.toJSONString(orderD, new SerializeConfig(true))
        MyKafkaSink.send("DWD_ORDER_DETAIL",orderD.order_id.toString,orderDstr)
      }
    )



    ssc.start()
    ssc.awaitTermination()


  }

}
