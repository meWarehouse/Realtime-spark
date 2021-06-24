package com.at.mall.realtime.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * @author zero
 * @create 2021-04-09 21:00
 */
object MysqlUtil {


  def main(args: Array[String]): Unit = {

    val list: List[JSONObject] = queryList("select * from offset_2021")

    println(list)



  }


  def queryList(sql: String): List[JSONObject] = {
    Class.forName("com.mysql.jdbc.Driver")
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_report?characterEncoding=utf-8&useSSL=false","root","root")
    val stat: Statement = conn.createStatement
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData
    while (rs.next) {
      val rowData = new JSONObject();
      for (i <- 1 to md.getColumnCount) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList += rowData
    }

    stat.close()
    conn.close()
    resultList.toList
  }




}
