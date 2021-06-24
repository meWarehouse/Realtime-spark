package com.at.mall.realtime.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

/**
 * @author zero
 * @create 2021-04-06 13:13
 */
object MyKafkaSink {

  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val broker_list: String = properties.getProperty("kafka.broker.list")

  var kafkaProducer:KafkaProducer[String,String] = null

  def createKafkaProducer: KafkaProducer[String, String] = {

    val prop = new Properties
    prop.put("bootstrap.servers", broker_list)
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("enable.idompotence",(true: java.lang.Boolean))

    var producer:KafkaProducer[String,String] = null

    try {
      producer = new KafkaProducer[String, String](prop)
    } catch {
      case e:Exception =>
      e.printStackTrace()
    }

    producer
  }

  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))

  }

  def send(topic: String,key:String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic,key, msg))

  }


}
