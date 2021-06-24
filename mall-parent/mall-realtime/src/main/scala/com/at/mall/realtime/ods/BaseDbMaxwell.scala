package com.at.mall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.at.mall.realtime.util.{MyKafkaSink, MykafkaUtil, OffsetManager}
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
object BaseDbMaxwell {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("base_db_canal_app")

    val ssc = new StreamingContext(conf, Seconds(5))


    val topic: String = "MALL_DB_M"
    val groupId: String = "base_db_maxwell_group"

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
      rdd.foreach(jsonObj => {

//        println("BaseDbMaxwell 监控数据：")
        println(jsonObj)

        if(jsonObj.getJSONObject("data")!=null && !jsonObj.getJSONObject("data").isEmpty
          && !"delete".equals(jsonObj.getString("type")) &&
          (("order_info".equals(jsonObj.getString("table"))&&"insert".equals(jsonObj.getString("type") ))
            ||"order_detail".equals(jsonObj.getString("table"))
            ||"base_province".equals(jsonObj.getString("table"))
            ||"user_info".equals(jsonObj.getString("table"))
            ||"base_category3".equals(jsonObj.getString("table"))
            ||"base_trademark".equals(jsonObj.getString("table"))
            ||"spu_info".equals(jsonObj.getString("table"))
            ||"sku_info".equals(jsonObj.getString("table"))
            )
        ){
          val jsonArr: String = jsonObj.getString("data")

          val topicD: String = "ODS_" + jsonObj.getString("table").toUpperCase()
//          Thread.sleep(500)
          MyKafkaSink.send(topicD,jsonArr) //非幂等的操作 可能会导致数据重复
        }



      })
    }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }




    ssc.start()
    ssc.awaitTermination()





  }


}
