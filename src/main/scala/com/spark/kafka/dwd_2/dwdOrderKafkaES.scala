package com.spark.kafka.dwd_2
import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.fasterxml.jackson.databind.DeserializationConfig
import com.spark.kafka.bean.{BaseProvince, DauInfo, OrderDetail, OrderInfo, OrderWide, UserInfo}
import com.spark.kafka.util.{DateUtil, EsUtil, JSONUtil, JedisUtil, MyBeanUtil, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable.ListBuffer

object dwdOrderKafkaES {

  def main(args: Array[String]): Unit = {
    //获取环境
    val conf = new SparkConf().setAppName("orderwide").setMaster("local[4]")
    val ssc = new StreamingContext(conf = conf, batchDuration = Durations.seconds(5))
    //获取偏移量
    var topic1:String="DWD_ORDER_INFO_I"
    var topic2:String="DWD_ORDER_DETAIL_I"
    var group="DWD_ORDER_group"
    //source获取kafka主题
    val orderInfoOffset:Map[TopicPartition,Long] = OffsetManagerUtil.getOffset(topic1, group)
    val orderDetailOffset:Map[TopicPartition,Long] = OffsetManagerUtil.getOffset(topic2, group)

    var orderInfoDS:DStream[ConsumerRecord[String,String]]=null
    if(orderInfoOffset!=null&&orderInfoOffset.nonEmpty){
      orderInfoDS=MyKafkaUtil.getKafkaStreamWithOffset(ssc,topic1,group,orderInfoOffset)
    }else{
      orderInfoDS=MyKafkaUtil.getKafkaStream(ssc,topic1,group)
    }
    var orderDetailDS:DStream[ConsumerRecord[String,String]]=null
    if(orderDetailOffset!=null&&orderDetailOffset.nonEmpty){
      orderDetailDS=MyKafkaUtil.getKafkaStreamWithOffset(ssc,topic2,group,orderDetailOffset)
    }else{
      orderDetailDS=MyKafkaUtil.getKafkaStream(ssc,topic2,group)
    }
    //提取偏移量结束点
//    var offsets1:Array[OffsetRange]=null
//    var offsets2:Array[OffsetRange]=null
//    orderInfoDS=orderInfoDS.transform(rdd=>{
//      offsets1=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd
//    })
//    orderDetailDS=orderDetailDS.transform(rdd=>{
//      offsets2=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd
//    })
    //map转换为javabean

    val orderInfoBeanDS:DStream[OrderInfo] = orderInfoDS.filter(json => JSONUtil.validate(json.value())).map(json => {
      val str = json.value()
      val jSONObject = JSON.parseObject(str)
      val orderInfoJson = jSONObject.getString("data")
      val info = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
      info
    })
    val orderDetailBeanDS :DStream[(Long,OrderDetail)]= orderDetailDS.filter(json => JSONUtil.validate(json.value())).map(json => {
      val str = json.value()
      val jSONObject = JSON.parseObject(str)
      val orderDetailJson = jSONObject.getString("data")
      val detail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
      (detail.order_id,detail)
    })
    //关联dim
    val orderInfoWithDimDS :DStream[(Long,OrderInfo)]= orderInfoBeanDS.mapPartitions(iter => {
      val jedis = JedisUtil.getJedis()
      val list = iter.toList
      val listBuffer = new ListBuffer[(Long, OrderInfo)]
      for (orderInfo <- list) {
        //关联用户维度
        val dimUserKey = s"DIM:USER_INFO:${orderInfo.user_id}"
        val userInfoJson: String = jedis.get(dimUserKey)
//        JedisUtil
//          .getIfNullFromMysql(jedis,"DIM:USER_INFO",classOf[UserInfo], 3000, "id")
        val userInfoJsonObj: JSONObject =
          JSON.parseObject(userInfoJson)
        //提取生日
        val birthday: String =
          userInfoJsonObj.getString("birthday")
        //提取性别
        val gender: String = userInfoJsonObj.getString("gender")
        //生日处理为年龄
        var age: String = null
        if (birthday != null) {
          //闰年无误差
          val birthdayDate: LocalDate =
            DateUtil.getLocalDate(birthday)
          val nowDate: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayDate,
            nowDate)
          val years: Int = period.getYears
          age = years.toString
        }
        orderInfo.user_age = age.toInt
        orderInfo.user_gender = gender


        //关联地区维度
        val provinceJson = JedisUtil.getIfNullFromMysql(jedis, "DIM:BASE_PROVINCE", classOf[BaseProvince],
          orderInfo.province_id, "id")
        if (provinceJson != null && provinceJson.nonEmpty) {
          val provinceJsonObj: JSONObject = {
            JSON.parseObject(provinceJson)

          }
          orderInfo.province_id = provinceJsonObj.getString("id").toLong
          orderInfo.province_name =
            provinceJsonObj.getString("name")
          orderInfo.province_area_code =
            provinceJsonObj.getString("area_code")
          orderInfo.province_3166_2_code =
            provinceJsonObj.getString("iso_3166_2")
          orderInfo.province_iso_code =
            provinceJsonObj.getString("iso_code")
        }
        val createTimeArr = orderInfo.create_time.split(" ")
        orderInfo.create_date = createTimeArr(0)
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        listBuffer.append((orderInfo.id,orderInfo))
      }
      jedis.close()
      listBuffer.toIterator
    })
//    orderInfoWithDimDS.print()
//    orderDetailBeanDS.print()
    //两流join,由于两边的数据可能会有延迟，会出现数据丢失，不用inner join
    val joinOrderDS:DStream[(Long,(Option[OrderInfo],Option[OrderDetail]))] = {
      orderInfoWithDimDS.fullOuterJoin(orderDetailBeanDS)

}

    val joinOrderWideDS = joinOrderDS.flatMap {
      case (order_id, (orderInfoOpt, orderDetailOpt)) => {
        val jedis = JedisUtil.getJedis()
        val orderWides = new ListBuffer[OrderWide]
        //先看主表在不在
        if (orderInfoOpt != null) {
          val orderInfo = orderInfoOpt.get

          if (orderDetailOpt != null) {
            val orderDetail = orderDetailOpt.get
            val wide = new OrderWide(orderInfo, orderDetail)
            orderWides.append(wide)
          }
          //无论上面是否join到orderDetail，尝试读缓存(之前的orderDetail都可能在缓存中)
          import scala.collection.JavaConverters._
          var orderDetailKey = s"orderJoin:orderDetail:$order_id"
          val orderDetails: util.Set[String] = jedis.smembers(orderDetailKey)
          val orderDetailSet: Set[String] = orderDetails.asScala.toSet
          if (orderDetails != null && orderDetailSet.nonEmpty) {
            for (orderDetailJson <- orderDetailSet) {
              val detail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              orderWides.append(new OrderWide(orderInfo, detail))
            }
          }

          //虽说上面可能匹配成功了一个orderwide，但可能有其他orderDetail没来
          //主表写入缓存
          var key = s"orderJoin:orderInfo:$order_id"
          jedis.set(key, JSON.toJSONString(orderInfo, new SerializeConfig(true)))
          jedis.expire(key, 3600 * 24)


        }
        //主表不在， 看从表
        else {
          val orderDetail = orderDetailOpt.get

          //先读缓存，而不是先写
          var key = s"orderJoin:orderInfo:${orderDetail.order_id}"
          val orderInfoJson = jedis.get(key)
          if (orderInfoJson != null) {
            val orderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            orderWides.append(new OrderWide(orderInfo, orderDetail))
          } else {
            var orderDetailKey = s"orderJoin:orderDetail:${orderDetail.order_id}"
            jedis.sadd(orderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
            jedis.expire(orderDetailKey, 3600 * 24)
          }

        }
        jedis.close()
        orderWides


      }

    }
   // sink rdd遍历->提交偏移量
    joinOrderWideDS.foreachRDD(rdd=>{
      rdd.foreachPartition{
        iter=>{

          val orderWides:List[(String,OrderWide)] = iter.map(orderWide=>(orderWide.detail_id.toString,orderWide)).toList
          if(orderWides!=null&&orderWides.nonEmpty){
          val dt = orderWides.head._2.create_date
          var indexName=s"gmall_order_wide_$dt"
          EsUtil.saveBulkIdempotent(orderWides,indexName)
        }}
      }
//      OffsetManagerUtil.saveOffset(topic1,group,offsets1)
//      OffsetManagerUtil.saveOffset(topic2,group,offsets2)
    })
    joinOrderWideDS.print()




//    joinOrderWideDS.print()
    //执行
    ssc.start()
    ssc.awaitTermination()
  }

}
