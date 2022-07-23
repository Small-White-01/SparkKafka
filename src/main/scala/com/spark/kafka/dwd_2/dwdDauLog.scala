package com.spark.kafka.dwd_2

import com.alibaba.fastjson.{JSON, JSONObject}
import com.spark.kafka.bean.{BaseProvince, DauInfo, PageLog}
import com.spark.kafka.util.{DateUtil, EsUtil, JSONUtil, JedisUtil, MyBeanUtil, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, Period}
import java.util.{Calendar, Date}
import scala.collection.mutable.ListBuffer

object dwdDauLog {

  def main(args: Array[String]): Unit = {
    recoveryState()
//PageLog(mid_3,1002,310000,Appstore,0,iPhone Xs,iOS 13.3.1,v2.1.134,cart,good_detail,null,null,16147,1608619645000)
    //获取环境
    val sparkCnf=new SparkConf().setMaster("local[4]").setAppName("dwdDauLog")
    val ssc=new StreamingContext(sparkCnf,Durations.seconds(5))
    //获取偏移量
    val topic="DWD_PAGE_LOG"
    val group="DWD_PAGE_LOG_group"
    val offsets = OffsetManagerUtil.getOffset(topic, group)
    var kafkaDStream: DStream[ConsumerRecord[String, String]] = null
    if(offsets!=null&&offsets.nonEmpty){
      kafkaDStream=MyKafkaUtil.getKafkaStreamWithOffset(ssc,topic,group,offsets)
    }
    else{
      kafkaDStream=MyKafkaUtil.getKafkaStream(ssc,topic,group)
    }
    //提取偏移量结束点
    var offsetRanges:Array[OffsetRange]=null
    kafkaDStream=kafkaDStream.transform(rdd=>{
      offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })


    val pageLogStream = kafkaDStream.filter(json=>{
      JSONUtil.validate(json.value())
    }).map(json => {
      JSON.parseObject(json.value(), classOf[PageLog])
    })

    /**
     *

    //    pageLogStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(iter=>{
//        val list = iter.toList
//        for (json<-list){
//          println(json)
//        }
//
//      })
//
//    })
     */
    val filterPageLogStream = pageLogStream.filter(pagelog => pagelog.last_page_id != null)
    //分区进行统计日活,以mid为key，并设置状态，表明这一天已来访
    val pageLogFilterStream = filterPageLogStream.mapPartitions(iter => {
      val jedis = JedisUtil.getJedis()
      val list = iter.toList
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val listBuffer: ListBuffer[PageLog] = new ListBuffer()
      println("过滤前 : " + list.size)
      for (pageLog <- list) {

        val date = format.format(new Date(pageLog.ts))
        val key = s"DAU:"+s"$date"
        //由于日志mid有数万个甚至更多，用set 会导致redis存储几万个key
        //最好使用集合存储，还可以去重
//        if (!jedis.exists(key)) {
//          jedis.set(key, "1")
//          listBuffer.append(pageLog)
//        }
        val res = jedis.sadd(key, pageLog.mid)
        if(res==1){
          listBuffer.append(pageLog)
        }
        jedis.expire(key, 60 * 60 * 24)


      }
      jedis.close()
      println("过滤后 : " + listBuffer.size)
      listBuffer.toIterator
    })
    val dauLogStream = pageLogFilterStream.mapPartitions(iter => {
      val jedis: Jedis = JedisUtil.getJedis()
      val dauInfoList: ListBuffer[DauInfo] =
        ListBuffer[DauInfo]()
      for (pageLog <- iter.toList) {
        //用户信息关联
        val dimUserKey = s"DIM:USER_INFO:${pageLog.user_id}"
        val userInfoJson: String = jedis.get(dimUserKey)
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
        val dauInfo = new DauInfo()
        //将 PageLog 的字段信息拷贝到 DauInfo 中
        MyBeanUtil.copyProperties(pageLog, dauInfo)
        dauInfo.user_gender = gender
        dauInfo.user_age = age
        //TODO 地区维度关联
        val provinceKey: String = {
          s"DIM:BASE_PROVINCE:${pageLog.province_area_code}"

        }

        val provinceJson: String = JedisUtil.getIfNullFromMysql(jedis,"DIM:BASE_PROVINCE",classOf[BaseProvince],
          pageLog.province_area_code,"area_code")
        if (provinceJson != null && provinceJson.nonEmpty) {
          val provinceJsonObj: JSONObject = {
            JSON.parseObject(provinceJson)

          }
          dauInfo.province_id= provinceJsonObj.getString("id")
          dauInfo.province_name =
            provinceJsonObj.getString("name")
          dauInfo.province_area_code =
            provinceJsonObj.getString("area_code")
          dauInfo.province_3166_2 =
            provinceJsonObj.getString("iso_3166_2")
          dauInfo.province_iso_code =
            provinceJsonObj.getString("iso_code")
        }
        //日期补充
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val dtDate = new Date(dauInfo.ts)
        val dtHr: String = dateFormat.format(dtDate)
        val dtHrArr: Array[String] = dtHr.split(" ")
        dauInfo.dt = dtHrArr(0)
        dauInfo.hr = dtHrArr(1)
        dauInfoList.append(dauInfo)

      }
      jedis.close()
      dauInfoList.toIterator
    })

    dauLogStream.foreachRDD(rdd=>{

      rdd.foreachPartition(iter=>{
        val list:List[(String,DauInfo)] = iter.map(dau => (dau.mid, dau))
          .toList
        println(list)
        if(list.nonEmpty){
          val tuple = list.head
          val dt = tuple._2.dt
          val index=s"gmall_dau_info_$dt"
          EsUtil.saveBulkIdempotent(list,index)
        }


      })

      OffsetManagerUtil.saveOffset(topic,group,offsetRanges)
    })


    //执行}
    ssc.start()
    ssc.awaitTermination()

  }
  //写入es时挂掉后
  def recoveryState(): Unit ={
    val date = LocalDate.now()
    //从es中搜索已经写入的mid文档
    val midList:Array[String] = EsUtil.search(s"gmall_dau_info_$date", "mid")
    //由于redis中已经存在mid数据，会被过滤掉，
    //del key
    val jedis = JedisUtil.getJedis()
    val key = s"DAU:"+s"$date"
    jedis.del(key)
    //从es查询得到的mid写入redis
    if(midList!=null&&midList.length>0) {

      val pipeline = jedis.pipelined()
      for (mid <- midList) {
        pipeline.sadd(key, mid)
      }

      val now = LocalDateTime.of(LocalDate.now(),LocalTime.now())
      val of = LocalDateTime.of(LocalDate.now(),LocalTime.of(23,59,59))
      val duration = Duration.between(now, of)
      pipeline.expire(key, duration.getSeconds.asInstanceOf[Int])
      pipeline.sync()

    }

    jedis.close()


  }

}
