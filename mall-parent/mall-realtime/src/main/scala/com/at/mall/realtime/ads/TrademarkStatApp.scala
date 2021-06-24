package com.at.mall.realtime.ads


import com.alibaba.fastjson.JSON
import com.at.mall.realtime.bean.OrderDetailWide
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
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author zero
 * @create 2021-04-09 21:11
 */
object TrademarkStatApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("trademark_stat_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))


    val topic = "DWS_ORDER_WIDE";
    val groupId = "trademark_stat_group"

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


    val orderDetailWideDS: DStream[OrderDetailWide] = recordInputDS.map { record =>
      JSON.parseObject(record.value(), classOf[OrderDetailWide])
    }

    val amountWithTmDstream: DStream[(String, Double)] = orderDetailWideDS.map(orderWide =>
      (orderWide.tm_id + ":" + orderWide.tm_name, orderWide.final_detail_amount))

    val amountByTmDstream: DStream[(String, Double)] = amountWithTmDstream.reduceByKey(_ + _)

    amountByTmDstream.print(1000)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //架构数据写入mysql
    amountByTmDstream.foreachRDD{rdd =>

      val amountArray: Array[(String, Double)] = rdd.collect()


      if(amountArray != null && amountArray.size > 0){

        DBs.setup()
        DB.localTx(implicit session => {
          // 此括号内的代码 为原子事务
          for ((tm,amount) <- amountArray ) {
            ///写数据库
            val tmArr: Array[String] = tm.split(":")
            val tmId=tmArr(0)
            val tmName=tmArr(1)
            val statTime: String = dateFormat.format(new Date())
            println("数据写入 执行")
            SQL("insert into trademark_amount_stat values (?,?,?,?) ").bind(statTime,tmId,tmName,amount).update().apply()
          }


//          throw new RuntimeException("触发异常")


          //sql2  //提交偏移量
          for (offsetRange <- offsetRanges ) {
            val partitionId: Int = offsetRange.partition
            val untilOffset: Long = offsetRange.untilOffset
            println("偏移量提交 执行")
            SQL("REPLACE INTO  offset_2021(group_id,topic,partition_id,topic_offset)  VALUES(?,?,?,?) ")
              .bind(groupId,topic,partitionId,untilOffset).update().apply()
          }



        })

      }


    }



    ssc.start()
    ssc.awaitTermination()


  }


}
