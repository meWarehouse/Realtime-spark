package com.at.mall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.at.mall.realtime.util.{MyKafkaSink, MykafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zero
 * @create 2021-04-06 12:37
 */
object BaseDbCanal {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("base_db_canal_app")

    val ssc = new StreamingContext(conf, Seconds(5))


    val topic: String = "MALL_DB_C"
    val groupId: String = "base_db_canal_group"

    var recordInputDS: InputDStream[ConsumerRecord[String, String]] = null

    //获取偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    if(kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      recordInputDS = MykafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    }else{
      recordInputDS = MykafkaUtil.getKafkaStream(topic,ssc,groupId)
    }


    //获取数据读取范围
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordInputDStream: DStream[ConsumerRecord[String, String]] = recordInputDS.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val jsonDS: DStream[JSONObject] = recordInputDStream.map(record => {
      val str: String = record.value()
      val jSONObject: JSONObject = JSON.parseObject(str)
      jSONObject
    })

    jsonDS.foreachRDD{rdd => {

      //将数据推回kafka
      rdd.foreach(jsonObject => {

        val jsonArr: JSONArray = jsonObject.getJSONArray("data")

        val topicD: String = "ODS_" + jsonObject.getString("table").toUpperCase()


        import scala.collection.JavaConversions._

        for (jsonObj <- jsonArr){
          val msg: String = jsonObj.toString

          MyKafkaSink.send(topicD,msg) //非幂等的操作 可能会导致数据重复
        }


      })
    }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }




    ssc.start()
    ssc.awaitTermination()





  }


}
