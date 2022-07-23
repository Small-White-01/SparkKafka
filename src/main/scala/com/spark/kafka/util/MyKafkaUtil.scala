package com.spark.kafka.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties
import scala.collection.mutable



object MyKafkaUtil {


  def flush(): Unit = {
    if(kafkaProducer!=null) {
     kafkaProducer.flush()
    }
  }

  //  /"use_a_separate_group_id_for_each_stream"
  val kafkaParams = mutable.Map[String,String](
    "bootstrap.servers" -> PropertiesUtil.getString("kafka.broker_list.url"),
    "key.deserializer" -> classOf[StringDeserializer].getName,
    "value.deserializer" -> classOf[StringDeserializer].getName,
    "group.id" -> "",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> "false"
  )

  def main(args: Array[String]): Unit = {
    println(kafkaParams("key.deserializer"))
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
  }



  //source
  def getKafkaStream(streamingContext: StreamingContext,topic:String,groupId:String)
  :InputDStream[ConsumerRecord[String,String]]={
    kafkaParams("group.id")=groupId
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
    )
    stream
  }
  def getKafkaStreamWithOffset(streamingContext: StreamingContext,
                               topic:String,groupId:String,
                               offsets:Map[TopicPartition,Long])
  :InputDStream[ConsumerRecord[String,String]]={
    kafkaParams("group.id")=groupId
    //kafkaParams("enable.auto.commit")="false"
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams,offsets)
    )

    stream
  }

  var kafkaProducer:KafkaProducer[String,String]=createKafkaProducer()

  //sink
  def createKafkaProducer():KafkaProducer[String,String]={

    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaParams("bootstrap.servers"));
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName);
    val kafkaProducer:KafkaProducer[String,String]
          =new KafkaProducer[String,String](properties)
          kafkaProducer
  }

  def send(topic:String,json:String)={
    kafkaProducer.send(new ProducerRecord[String,String](topic,json))
  }
  def send(topic: String, key: String, json: String) = {
    kafkaProducer.send(new ProducerRecord[String,String](topic,key,json))
  }
}
