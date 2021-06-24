package com.at.mall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.at.mall.realtime.bean.DauInfo
import com.at.mall.realtime.util
import com.at.mall.realtime.util.{ESUtil, MykafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @author zero
 * @create 2021-04-04 20:56
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(3))


    val groupId: String = "DAU_GROUP"
    val toipc: String = "GMALL_START"

    //从redis中读取偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(toipc, groupId)
    var recoders: InputDStream[ConsumerRecord[String, String]] = null
    if(kafkaOffsetMap != null && kafkaOffsetMap.size > 0){
      //redis 中有偏移量
      recoders = MykafkaUtil.getKafkaStream(toipc,ssc,kafkaOffsetMap,groupId)
    }else{
      //刚开始消费 redis 中没有偏移量
      recoders =  MykafkaUtil.getKafkaStream(toipc, ssc)
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var offsetRanges = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recoders.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      println("offsetRanges:-------------:"+offsetRanges.mkString("->"))
      rdd
    })



    val jsonObjectDS: DStream[JSONObject] = inputGetOffsetDstream.map(recoder => {
      val jSONObject: JSONObject = JSON.parseObject(recoder.value())

      val ts: lang.Long = jSONObject.getLong("ts")
      val date: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val hour: Array[String] = date.split(" ")

      jSONObject.put("dt", hour(0))
      jSONObject.put("hr", hour(1))

      jSONObject
    })

    //去重思路： 利用redis保存今天访问过系统的用户清单
    //清单在redis中保存
    //redis :  type set   string hash list set zset       key ? dau:2020-06-17        value?  mid   (field? score?)  (expire?) 24小时
    /*
    val filterDS: DStream[JSONObject] = jsonObjectDS.filter {
      jsonObj => {

        //每次获取redis连接消耗过大
        val redisClient: Jedis = RedisUtil.getJedisClient

        val dt: String = jsonObj.getString("dt")

        val mid: String = jsonObj.getJSONObject("common").getString("mid")

        val key: String = "dau:" + dt

        //sadd 不存在当前key 则保存并返回1 存在不保存返回0
        val isNew: lang.Long = redisClient.sadd(key, mid)

        redisClient.close()

        if (isNew == 1L) true else false

      }
    }
    */

    val filterDS: DStream[JSONObject] = jsonObjectDS.mapPartitions {

      jsonIter => {

        val toList: List[JSONObject] = jsonIter.toList

        val listBuffer = new ListBuffer[JSONObject]

        val redisClient: Jedis = RedisUtil.getJedisClient

//        println("过滤前：" + toList.size)

        //遍历迭代器中的数据 此迭代器只能被遍历一次
        for (elem <- toList) {
          val dt: String = elem.getString("dt")
          val mid: String = elem.getJSONObject("common").getString("mid")

          val key: String = "dau:" + dt

          val isNew: lang.Long = redisClient.sadd(key, mid)
          redisClient.expire(key, 3600 * 24)

          if (isNew == 1L) listBuffer.append(elem)

        }

        redisClient.close()
//        println("过滤后：" + listBuffer.size)
        listBuffer.toIterator

      }

    }



    //将过滤后的数据保存的es中
    filterDS.foreachRDD{rdd => {
      rdd.foreachPartition(partitionDatas => {

        val list: List[JSONObject] = partitionDatas.toList

        val dauInfoes: List[(String,DauInfo)] = list.map(elem => {
          val nObject: JSONObject = elem.getJSONObject("common")

          val dauInfo: DauInfo = DauInfo(
            nObject.getString("mid"),
            nObject.getString("uid"),
            nObject.getString("ar"),
            nObject.getString("ch"),
            nObject.getString("vc"),
            elem.getString("dt"),
            elem.getString("hr"),
            "00",
            elem.getLong("ts")
          )
          (dauInfo.mid,dauInfo)
        })

        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        ESUtil.bulkDoc(dauInfoes, "mall2021_dau_info" + dt)

      })
    }

      //保存偏移量
      OffsetManager.saveOffset(toipc,groupId,offsetRanges)

  }




    ssc.start()
    ssc.awaitTermination()


  }

}
