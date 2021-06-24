package com.at.mall.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author zero
 * @create 2021-04-04 20:27
 */
object PropertiesUtil {


  def load(propertiesName:String):Properties ={
    val properties = new Properties()

    properties.load(
      new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),"UTF-8")
    )

    properties
  }

  def main(args: Array[String]): Unit = {


    println(load("config.properties").getProperty("kafka.broker.list"))

  }


}
