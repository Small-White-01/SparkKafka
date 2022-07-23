package com.spark.kafka.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util


object OffsetManagerUtil {

  def getOffset(topic:String,groupId:String):Map[TopicPartition,Long]={
    val jedis = JedisUtil.getJedis()
    val key:String=topic+":"+groupId
    val stringToString:java.util.Map[String,String] = jedis.hgetAll(key)
    jedis.close()
    import scala.collection.JavaConverters._
    val map:Map[TopicPartition,Long] = stringToString.asScala.map {
      case (partitionId, offset) => {
        (new TopicPartition(topic, partitionId.toInt), offset.toLong)

      }
    }.toMap
    map
  }

  def saveOffset(topic:String,groupId:String,offsets:Array[OffsetRange])={
    val map = new util.HashMap[String, String]
    for(i<-offsets.indices){
      val offsetRange = offsets(i)
      val partition = offsetRange.partition
      val end_offset = offsetRange.untilOffset
      map.put(partition.toString,end_offset.toString)
    }
    if(!map.isEmpty) {
      val jedis = JedisUtil.getJedis()
      import scala.collection.JavaConverters._
      val key: String = topic + ":" + groupId
      jedis.hmset(key, map)
      jedis.close()
    }
  }

  def main(args: Array[String]): Unit = {



  }

}
