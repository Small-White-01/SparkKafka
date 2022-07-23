package com.spark.kafka.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchRequestBuilder}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.common.xcontent.json.JsonXContentParser
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object EsUtil {



  val restClient:RestHighLevelClient=create()



  def create(): RestHighLevelClient = {
    val builder = RestClient.builder(new HttpHost("hadoop2", 9200), new HttpHost("hadoop3", 9200),
      new HttpHost("hadoop4", 9200))
    val client = new RestHighLevelClient(builder)
    client

  }

  def search(index:String,field:String): Array[String] ={
    val getIndexRequest = new GetIndexRequest(index)

    if(!restClient.indices.exists(getIndexRequest,RequestOptions.DEFAULT))return null


    val builder = new SearchSourceBuilder()
    //指定抓取哪些字段，不用非得全部抓取
    builder.fetchSource(field,null).size(10000)
    val request = new SearchRequest(index)

    request.source(builder)
    val response = restClient.search(request, RequestOptions.DEFAULT)
    val hits:Array[SearchHit] = response.getHits.getHits
    val len = response.getHits.getTotalHits.value.asInstanceOf[Int]
    if(len>0) {
      val midArray = new Array[String](len)
      for (i <- 0 until (len)) {
        val hit: SearchHit = hits(i)
        val map: java.util.Map[String, AnyRef] = hit.getSourceAsMap
        midArray(i) = map.get(field).toString
      }
      return midArray
    }
    null
  }


  def saveBulkIdempotent(sources:List[(String,AnyRef)],indexName:String): Unit ={
    val bulkRequest = new BulkRequest()
    if(sources!=null&&sources.nonEmpty) {
      for (source<-sources) {
        val request = new IndexRequest()
        request.index(indexName)
        request.source(JSON.toJSONString(source._2, new SerializeConfig(true)), XContentType.JSON)
        request.id(source._1)
        bulkRequest.add(request)
      }
      restClient.bulk(bulkRequest,RequestOptions.DEFAULT)
    }

  }

  def saveBulk(sources:List[AnyRef],indexName:String): Unit ={
    val bulkRequest = new BulkRequest()
    if(sources!=null&&sources.nonEmpty) {
      for (source<-sources) {
        val request = new IndexRequest()
        request.index(indexName)
        request.source(JSON.toJSONString(source, new SerializeConfig(true)), XContentType.JSON)
        bulkRequest.add(request)
      }
      restClient.bulk(bulkRequest,RequestOptions.DEFAULT)
    }

  }
  def saveIdempotent(source:(String,AnyRef),indexName:String): Unit ={
    val request = new IndexRequest()
    request.index(indexName)
    request.source(JSON.toJSONString(source._2,new SerializeConfig(true)),XContentType.JSON)
    request.id(source._1)
    restClient.index(request,RequestOptions.DEFAULT)
  }

  def save(source:AnyRef,indexName:String): Unit ={
    val request = new IndexRequest()
    request.index(indexName)
    request.source(JSON.toJSONString(source,new SerializeConfig(true)),XContentType.JSON)
    restClient.index(request,RequestOptions.DEFAULT)
  }



}
