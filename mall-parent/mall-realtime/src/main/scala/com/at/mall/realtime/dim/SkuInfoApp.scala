package com.at.mall.realtime.dim


import com.alibaba.fastjson.{JSON, JSONObject}
import com.at.mall.realtime.bean.dim.UserInfo
import com.at.mall.realtime.util.{MykafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import com.at.mall.realtime.bean.dim._
import org.apache.spark.broadcast.Broadcast

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author zero
 * @create 2021-04-08 17:49
 */
object SkuInfoApp {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("dim_sku_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))


    val topic = "ODS_SKU_INFO";
    val groupId = "dim_sku_info_group"

    val kafkaOffset: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffset != null && kafkaOffset.size > 0) {
      recordInputStream = MykafkaUtil.getKafkaStream(topic, ssc, kafkaOffset, groupId)
    } else {
      recordInputStream = MykafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordInputDS: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val skuInfoDstream: DStream[SkuInfo] = recordInputStream.map { record =>
      JSON.parseObject(record.value(), classOf[SkuInfo])
    }
    //
    //    skuInfoDstream.mapPartitions{skuinfoIter =>
    //      val skuinfoList: List[SkuInfo] = skuinfoIter.toList
    //
    //      if(skuinfoList != null && skuinfoList.size > 0){
    //        val cate3Ids: List[String] = skuinfoList.map(_.category3_id)
    //        val spuIds: List[String] = skuinfoList.map(_.spu_id)
    //        val tmIds: List[String] = skuinfoList.map(_.tm_id)
    //
    //        var sql1:String = "select id,name from mall2021_base_category3 where id in ('"+cate3Ids.mkString("','")+"')"
    //        var sql2 = "select id,tm_name from mall2021_base_trademark where id in ('"+spuIds.mkString("','")+"')"
    //        var sql3 = "select id,spu_name from mall2021_spu_info where id in ('"+tmIds.mkString("','")+"')"
    //
    //
    //        val cat3Json: List[JSONObject] = PhoenixUtil.queryList(sql1)
    //        val spuJson: List[JSONObject] = PhoenixUtil.queryList(sql2)
    //        val tmJson: List[JSONObject] = PhoenixUtil.queryList(sql3)
    //
    //
    //      }

    val skuInfoDs: DStream[SkuInfo] = skuInfoDstream.transform { rdd =>

      if (rdd.count() > 0) {

        var sql1: String = "select id,name from mall2021_base_category3"
        var sql2 = "select id,tm_name from mall2021_base_trademark"
        var sql3 = "select id,spu_name from mall2021_spu_info"

        val cat3Json: List[JSONObject] = PhoenixUtil.queryList(sql1)
        val spuJson: List[JSONObject] = PhoenixUtil.queryList(sql2)
        val tmJson: List[JSONObject] = PhoenixUtil.queryList(sql3)

        val cat3Map: Map[String, JSONObject] = cat3Json.map(json => (json.getString("ID"), json)).toMap
        val spuMap: Map[String, JSONObject] = spuJson.map(json => (json.getString("ID"), json)).toMap
        val tmMap: Map[String, JSONObject] = tmJson.map(json => (json.getString("ID"), json)).toMap

        val dimList: List[Map[String, JSONObject]] = List[Map[String, JSONObject]](cat3Map, spuMap, tmMap)
        val dimBC: Broadcast[List[Map[String, JSONObject]]] = ssc.sparkContext.broadcast(dimList)

        rdd.mapPartitions { skuinfoIter =>

          val dimlist: List[Map[String, JSONObject]] = dimBC.value

          val cat3MapBC: Map[String, JSONObject] = dimlist(0)
          val spuMapBC: Map[String, JSONObject] = dimlist(1)
          val tmMapBC: Map[String, JSONObject] = dimlist(2)

          val skuInfoList: List[SkuInfo] = skuinfoIter.toList
          if (skuInfoList != null && skuInfoList.size > 0) {

            for (sku <- skuInfoList) {

              val cat3Object: JSONObject = cat3MapBC.getOrElse(sku.category3_id, null)
              if (cat3Object != null) {
                sku.category3_name = cat3Object.getString("NAME")
              }

              val spuObject: JSONObject = spuMapBC.getOrElse(sku.spu_id, null)
              if (spuObject != null) {
                sku.tm_name = spuObject.getString("TM_NAME")
              }

              val tmObject: JSONObject = tmMapBC.getOrElse(sku.tm_id, null)
              if (tmObject != null) {
                sku.spu_name = tmObject.getString("SPU_NAME")
              }
            }
          }
          skuInfoList.toIterator
        }

      } else rdd
    }

    skuInfoDs.print(1000)

    import org.apache.phoenix.spark._

    skuInfoDs.foreachRDD { rdd =>

      rdd.saveToPhoenix("MALL2021_SKU_INFO",
        Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"),
        new Configuration(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )

      OffsetManager.saveOffset(topic, groupId, offsetRanges)
    }


    ssc.start()
    ssc.awaitTermination()
  }

}



