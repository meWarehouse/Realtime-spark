package com.at.mall.realtime.dim

import com.alibaba.fastjson.JSON
import com.at.mall.realtime.util.{MykafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
import com.at.mall.realtime.bean.dim.ProvinceInfo

/**
 * @author zero
 * @create 2021-04-07 11:17
 */
object ProvinceApp {

  def main(args: Array[String]): Unit = {


    // 读取kafka中的省市的topic
    //加载流
    val sparkConf: SparkConf = new SparkConf().setAppName("province_app").setMaster("local[4]")
    sparkConf.set("spark.speculation", "false")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_BASE_PROVINCE"
    val groupId = "province_group"
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      recordInputStream = MykafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      recordInputStream = MykafkaUtil.getKafkaStream(topic, ssc, groupId)
    }


    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver? executor?  //周期性的执行
      rdd
    }


    // 写入到hbase中
    inputGetOffsetDstream.foreachRDD { rdd =>
      val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map { record =>
        val jsonString: String = record.value()
        val provinceInfo: ProvinceInfo = JSON.parseObject(jsonString, classOf[ProvinceInfo])

        provinceInfo
      }

      provinceInfoRDD.foreach(print)

      val value: RDD[ProvinceInfo] = provinceInfoRDD.filter(p => p!= null)

      value.saveToPhoenix("mall2021_province_info",
        Seq("ID", "NAME", "AREA_CODE", "ISO_CODE","ISO_3166_2"), new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
      OffsetManager.saveOffset(topic, groupId, offsetRanges)


    }

    ssc.start()
    ssc.awaitTermination()
  }
}
