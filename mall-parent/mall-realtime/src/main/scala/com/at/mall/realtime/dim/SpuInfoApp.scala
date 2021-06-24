package com.at.mall.realtime.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.at.mall.realtime.bean.dim.UserInfo
import com.at.mall.realtime.util.{MykafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import com.at.mall.realtime.bean.dim._

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author zero
 * @create 2021-04-08 17:48
 */
object SpuInfoApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("dim_spu_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))


    val topic = "ODS_SPU_INFO";
    val groupId = "dim_spu_info_group"

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

    val value: DStream[SpuInfo] = recordInputStream.map { record =>
      val str: String = record.value()
      val nObject: JSONObject = JSON.parseObject(str)
      SpuInfo(nObject.getString("id"), nObject.getString("spu_name"))
    }


    value.print(1000)

    import org.apache.phoenix.spark._

    value.foreachRDD { rdd =>

      rdd.saveToPhoenix("mall2021_spu_info", Seq("ID", "SPU_NAME"),
        new Configuration(),
        Some("hadoop102,hadoop103,hadoop104:2181"))

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }



    ssc.start()
    ssc.awaitTermination()


  }



}
