package com.at.mall.realtime.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition


/**
 * @author zero
 * @create 2021-04-05 14:17
 */
object OffsetManagerM {



  //从mysql中获取偏移量
  def getOffset(topicName:String,groupId:String):Map[TopicPartition,Long]={

    val sql: String = "SELECT partition_id,topic_offset FROM offset_2021 WHERE topic = '"+topicName+"' AND group_id='"+groupId+"'"

    val jsonObj: List[JSONObject] = MysqlUtil.queryList(sql)

    val topicpartitionmap: Map[TopicPartition, Long] = jsonObj.map { obj =>
      val partition = new TopicPartition(topicName, obj.getIntValue("partition_id"))
      val offset: Long = obj.getLongValue("topic_offset")

      (partition, offset)
    }.toMap


    topicpartitionmap

  }





}
