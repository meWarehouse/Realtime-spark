package com.at.mall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util

/**
 * @author zero
 * @create 2021-04-05 14:17
 */
object OffsetManager {

  /*

    kafka 偏移量管理

   */


  //从redis 中获取偏移量
  def getOffset(topicName:String,groupId:String):Map[TopicPartition,Long]={

    //Redis 中保存的结构
    //type:hash   key:"offset:[topic]:[groupid]" field:"" partition_id value offset expire
    val client: Jedis = RedisUtil.getJedisClient

    val offsetKey: String = "offset:" + topicName + ":" + groupId

    val offsetMap: util.Map[String, String] = client.hgetAll(offsetKey)
    client.close()

    import  scala.collection.JavaConversions._

//    if (offsetMap == null || offsetMap.size() == 0) return null
//    else {
      val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.map {
        case (partotionId, offset) => {
          println("加载分区的偏移量：" + partotionId + ":" + offset)
          (new TopicPartition(topicName, partotionId.toInt), offset.toLong)
        }
      }.toMap
      kafkaOffsetMap
//    }

  }


  //将偏移量保存到redis中
  def saveOffset(topicName:String,groupId:String,offsetRanges: Array[OffsetRange])={


    if(offsetRanges != null && offsetRanges.size > 0){

      val offsetKey: String = "offset:" + topicName + ":" + groupId

      import scala.collection.JavaConversions._
      val offsetMap: Map[String, String] = offsetRanges.map {
        offsetRange => {
          println("写入偏移量：" + offsetRange.partition + ":" + offsetRange.fromOffset + "->" + offsetRange.untilOffset)
          (offsetRange.partition + "", offsetRange.untilOffset + "")
        }
      }.toMap




      val client: Jedis = RedisUtil.getJedisClient

      client.hmset(offsetKey,offsetMap)

      client.close()

    }

  }




}
