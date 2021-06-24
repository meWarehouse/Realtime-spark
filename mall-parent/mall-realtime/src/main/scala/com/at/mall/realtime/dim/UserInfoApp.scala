package com.at.mall.realtime.dim

import com.alibaba.fastjson.JSON
import com.at.mall.realtime.bean.OrderInfo
import com.at.mall.realtime.util.{MykafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import com.at.mall.realtime.bean.dim._
import org.apache.hadoop.conf.Configuration

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author zero
 * @create 2021-04-08 16:46
 */
object UserInfoApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("dim_user_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))


    val topic = "ODS_USER_INFO";
    val groupId = "dim_user_info_group"

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


    val userInfoDstream: DStream[UserInfo] = recordInputStream.map { record =>
      val str: String = record.value()
      val userInfo: UserInfo = JSON.parseObject(str, classOf[UserInfo])
      userInfo
    }

    //构建用户信息
    val userinfoDS: DStream[UserInfo] = userInfoDstream.map { userinfo =>
      val bir: Date = new SimpleDateFormat("yyyy-MM-dd").parse(userinfo.birthday)
      val betweenMs: Long = System.currentTimeMillis() - bir.getTime

      val age: Long = betweenMs / 365L / 24L / 60L / 60L / 1000L

      if (age < 20) {
        userinfo.age_group = "20岁及以下"
      } else if (age > 30) {
        userinfo.age_group = "30岁以上"
      } else {
        userinfo.age_group = "21岁到30岁"
      }

      if (userinfo.gender == "M") {
        userinfo.gender_name = "男"
      } else {
        userinfo.gender_name = "女"
      }
      userinfo
    }

    userinfoDS.print(1000)

    //将user信息保存到hbase中
    userinfoDS.foreachRDD{rdd =>
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("MALL2021_USER_INFO",Seq("ID","USER_LEVEL","BIRTHDAY", "GENDER", "AGE_GROUP" , "GENDER_NAME"),new Configuration(),Some("hadoop102,hadoop103,hadoop104:2181"))

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()


  }

}
