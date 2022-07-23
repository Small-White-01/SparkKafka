package com.spark.kafka.ods_1
import com.alibaba.fastjson.JSON
import com.spark.kafka.util.{JedisUtil, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
object odsBaseDb {


  def main(args: Array[String]): Unit = {
    //获取环境
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("odsBaseDb")
    val ssc = new StreamingContext(sparkConf, Durations.seconds(5))





    var topic="ODS_BASE_DB_MAXWELL"
    var groupId="ODS_BASE_DB_MAXWELL_group"

    //从redis中获取偏移量
    val offset:Map[TopicPartition,Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var kafkaDStream:InputDStream[ConsumerRecord[String,String]]=null
    //若偏移量为空，则为初次启动应用，按kafkaStream默认的偏移量来
    if(offset!=null&&offset.nonEmpty){
      kafkaDStream=MyKafkaUtil.getKafkaStream(ssc,topic,groupId)
    }else{
      kafkaDStream=MyKafkaUtil.getKafkaStreamWithOffset(ssc,topic,groupId,offset)
    }

    //为每个rdd记录偏移量
    var offsetRanges:Array[OffsetRange]=null
    val kStream = kafkaDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    //每个转为JsonObject
    val jsonObjStream = kStream.map(json => {
      JSON.parseObject(json.value())
    })

//    jsonObjStream.mapPartitions(r)

    ssc.sparkContext.longAccumulator()
    jsonObjStream.foreachRDD(rdd=>{
      //从redis查询，设置dim表和fact表为广播变量,记录哪些是Dim表，哪些是fact表
      val jedis = JedisUtil.getJedis()
      val DIMTable:String="DIM:TABLE";
      val FactTable:String="FACT:TABLE"
      val DIM_TABLES = jedis.smembers(DIMTable)
      val FACT_TABLES = jedis.smembers(FactTable)
      jedis.close()
      val DIM_TABLE_BC = ssc.sparkContext.broadcast(DIM_TABLES)
      val FACT_TABLE_BC = ssc.sparkContext.broadcast(FACT_TABLES)
      rdd.foreachPartition(rdp=>{
          val jedis1 = JedisUtil.getJedis()
        println(rdp.hasNext)
//        rdp.foreach(jsonObj=>{
//
//        })
//        println("rdp:"+rdp.length)
        while (rdp.hasNext) {

          val jsonObject = rdp.next()

          //根据广播变量判断，dim表则实时更新id，fact表则根据opt分别写入kafka
          /**
           * {"database":"gmallflink","table":"cart_info","type":"insert","ts":1654614903,
           * "xid":6383,"xoffset":2890,"data":{"id":209226,"user_id":"2258","sku_id":9,
           * "cart_price":8197.00,"sku_num":2,"img_url":"http://47.93.148.192:8080/group1/M00/00/02/rBHu8l-rgfWAJllcAAEY0AkXL8M782.jpg",
           * "sku_name":"Apple iPhone 12 (A2404) 64GB 红色 支持移动联通电信5G Processed a total of 5641 messages
           */
          val tableName = jsonObject.getString("table").toUpperCase()
          val operate_type = jsonObject.getString("type")
          val opt:String=operate_type match {
            case "bootstrap-insert"=>"I"
            case "insert"=>"I"
            case "update"=>"U"
            case _=>null
          }
          if(opt!=null){
            val dataObject = jsonObject.getJSONObject("data")
            if(FACT_TABLE_BC.value.contains(tableName)){
              val sink_topic:String=s"DWD_${tableName}_$opt"
              val key = dataObject.getString("id")
              //分区发送
              MyKafkaUtil.send(sink_topic ,key,jsonObject.toJSONString)
              println(s"$sink_topic,$key,send"+jsonObject.toJSONString)


            }

            if(DIM_TABLE_BC.value.contains(tableName)){
              var id = dataObject.getString("id")
//              if(tableName.startsWith("BASE_PROVINCE")){
//                id=dataObject.getString("area_code")
//              }

              val key=s"DIM:$tableName:$id"
              jedis1.set(key,dataObject.toJSONString)
              println(s"$key:"+jsonObject.toJSONString)

            }



          }


        }
        jedis1.close()
        MyKafkaUtil.flush()



    })
    OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)




    })









    ssc.start()
//    ssc.checkpoint()
    ssc.awaitTermination()
  }

}
