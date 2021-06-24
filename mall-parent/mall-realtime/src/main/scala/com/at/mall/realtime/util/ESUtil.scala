package com.at.mall.realtime.util

import com.google.gson.JsonObject
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, DocumentResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.highlight.{HighlightBuilder, HighlightField}
import org.elasticsearch.search.sort.SortOrder

import java.util
import scala.collection.mutable.ListBuffer


/**
 * @author zero
 * @create 2021-04-04 17:23
 */
object ESUtil {

  private var factory: JestClientFactory = null;

  def getClient: JestClient = {
    if (factory == null) build();
    factory.getObject

  }

  def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build())

  }


  def insertTest():Unit={

    val client: JestClient = getClient

    val index = new Index.Builder( MovieTest("001", "yihe001")).index("movie_test_20210405").`type`("_doc").id("001").build()

    val message: String = client.execute(index).getErrorMessage

    if(message != null){
      println(message)
    }

    client.close()


  }

  def queryTest():Unit={


    val client: JestClient = getClient

    var q1:String = "{\n  \"query\": {\n    \"bool\": {\n      \"filter\": [\n        {\n          \"term\": {\n            \"actorList.id\": \"1\"\n          }\n        },\n        {\n          \"term\": {\n            \"actorList.id\": \"3\"\n          }\n        }\n      ],\n      \"must\": {\n        \"match\": {\n          \"name\": \"red\"\n        }\n      }\n    }\n  }\n}"


//    val search= new Search.Builder(q1).addIndex("movie_index").addType("movie").build()

    val sourceBuilder = new SearchSourceBuilder

    val boolQueryBuilder = new BoolQueryBuilder
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.id","1"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.id","3"))
    boolQueryBuilder.must(new MatchQueryBuilder("name","red"))
    sourceBuilder.query(boolQueryBuilder)


    val q2: String = sourceBuilder.toString

    val search= new Search.Builder(q2).addIndex("movie_index").addType("movie").build()

    val result: SearchResult = client.execute(search)

    val hits: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])

    var resultList:ListBuffer[util.Map[String,Any]]=new  ListBuffer[util.Map[String, Any]] ;
    import  scala.collection.JavaConversions._
    for ( hit <- hits ) {
      val source: util.Map[String, Any] = hit.source
      resultList.add(source)
    }
    println(resultList.mkString("\n"))

    client.close()

  }

 def bulkDoc(sourceList:List[(String,Any)],indexName:String)={

   if(sourceList != null && sourceList.size>0){

     val client: JestClient = getClient

     //构建批量操作
     val bulkBuilder = new Bulk.Builder

     for (elem <- sourceList) {

       val index: Index = new Index.Builder(elem._2).index(indexName).id(elem._1).`type`("_doc").build()
       bulkBuilder.addAction(index)

     }
     val result: BulkResult = client.execute(bulkBuilder.build())

     val items: util.List[BulkResult#BulkResultItem] = result.getItems
     println("保存到ES:"+items.size()+"条数")


     client.close()
   }

 }

  def indexDoc(source:Any,indexName:String)={

    if(source != null){

      val client: JestClient = getClient

      val result: DocumentResult = client.execute(new Index.Builder(source).index(indexName).`type`("_doc").build())

      val errorMessage: String = result.getErrorMessage

      if(errorMessage != null) println(errorMessage)

      client.close()
    }

  }





  def main(args: Array[String]): Unit = {

//    insertTest()

    queryTest()

  }


  case class MovieTest(id:String,name:String)


}
