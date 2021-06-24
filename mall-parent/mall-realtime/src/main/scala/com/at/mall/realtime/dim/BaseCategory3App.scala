package com.at.mall.realtime.dim

import com.alibaba.fastjson.JSON
import com.at.mall.realtime.bean.dim.BaseCategory3
import com.at.mall.realtime.util.{MykafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import com.at.mall.realtime.bean.dim._

/**
 * @author zero
 * @create 2021-04-08 17:48
 */
object BaseCategory3App {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("dim_base_category3_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))


    val topic = "ODS_BASE_CATEGORY3";
    val groupId = "base_category3_group"

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

    val cat3: DStream[BaseCategory3] = recordInputStream.map { record =>
      val str: String = record.value()
      val category3: BaseCategory3 = JSON.parseObject(str, classOf[BaseCategory3])
      category3
    }



    cat3.print(1000)

    import org.apache.phoenix.spark._

    cat3.foreachRDD { rdd =>

      rdd.saveToPhoenix("MALL2021_BASE_CATEGORY3", Seq("ID", "NAME", "CATEGORY2_ID"),
        new Configuration(),
        Some("hadoop102,hadoop103,hadoop104:2181"))

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }



    ssc.start()
    ssc.awaitTermination()


  }

}
