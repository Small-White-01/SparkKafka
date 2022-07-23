package com.spark.kafka.ods_1

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.spark.kafka.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.spark.kafka.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}


object odsBaseLog {


  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setAppName("odsBaseLog").setMaster("local[4]")
    val sparkCnt=new StreamingContext(sparkConf, Durations.seconds(5))


    //原始主题
    val ods_base_topic : String = "ODS_BASE_LOG"
    //启动主题
    val dwd_start_log : String = "DWD_START_LOG"
    //页面访问主题
    val dwd_page_log : String = "DWD_PAGE_LOG"
    //页面动作主题
    val dwd_page_action : String = "DWD_PAGE_ACTION"
    //页面曝光主题
    val dwd_page_display : String = "DWD_PAGE_DISPLAY"
    //错误主题
    val dwd_error_info : String ="DWD_ERROR_INFO"
    //消费组
    val group_id : String = "ods_base_log_group"

    var kafkaDStream:DStream[ConsumerRecord[String,String]] =null;
//    DStream.map(_.value()).print(3)
    val offsets:Map[TopicPartition,Long]= OffsetManagerUtil.getOffset(ods_base_topic, group_id)
    if(offsets.isEmpty){
      kafkaDStream=MyKafkaUtil.getKafkaStream(sparkCnt, ods_base_topic, group_id)
    }else{
      kafkaDStream=MyKafkaUtil.getKafkaStreamWithOffset(sparkCnt,ods_base_topic,group_id,offsets)
    }
    var offsetRanges:Array[OffsetRange]=null
    kafkaDStream=kafkaDStream.transform(rdd=>{
      offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val jsonDStream = kafkaDStream.map((rec) => {
      val json = rec.value()
      val jsonObject = JSON.parseObject(json)
      jsonObject
    })

    jsonDStream.cache()


    jsonDStream.foreachRDD(rdd=> {
      rdd.foreach(jsonObject => {
        val err = jsonObject.getString("err")
        if (err != null) {
          MyKafkaUtil.send(dwd_error_info, err)
        } else {
          val commonObj = jsonObject.getJSONObject("common")
          val mid: String = commonObj.getString("mid")
          val uid: String = commonObj.getString("uid")
          val ar: String = commonObj.getString("ar")
          val ch: String = commonObj.getString("ch")
          val os: String = commonObj.getString("os")
          val md: String = commonObj.getString("md")
          val vc: String = commonObj.getString("vc")
          val isNew: String = commonObj.getString("is_new")
          val ts: Long = jsonObject.getLong("ts")

          //分流页面日志
          val pageObj: JSONObject = jsonObject.getJSONObject("page")
          if (pageObj != null) {
            //提取字段
            val pageId: String = pageObj.getString("page_id")
            val pageItem: String = pageObj.getString("item")
            val pageItemType: String =
              pageObj.getString("item_type")
            val lastPageId: String =
              pageObj.getString("last_page_id")
            val duringTime: Long = pageObj.getLong("during_time")
            //封装 bean
            val pageLog =
              PageLog(mid, uid, ar, ch, isNew, md, os, vc, pageId,
                lastPageId, pageItem, pageItemType, duringTime, ts)
            //发送 kafka
            MyKafkaUtil.send(dwd_page_log, JSON.toJSONString(pageLog, new
                SerializeConfig(true)))


            val actionArr: JSONArray = jsonObject.getJSONArray("actions")
            if (actionArr != null && actionArr.size() > 0) {
              for (i <- 0 until (actionArr.size())) {
                val actionObj = actionArr.getJSONObject(i)

                val actionId: String =
                  actionObj.getString("action_id")
                val actionItem: String =
                  actionObj.getString("item")
                val actionItemType: String =
                  actionObj.getString("item_type")
                //TODO actionts
                val actionTs: Long = actionObj.getLong("ts")
                //封装 Bean
                val pageActionLog =
                  PageActionLog(mid, uid, ar, ch, isNew, md, os, vc,
                    pageId, lastPageId, pageItem, pageItemType,
                    duringTime, actionId, actionItem, actionItemType, actionTs)
                MyKafkaUtil.send(dwd_page_action, JSON.toJSONString(pageActionLog, new
                    SerializeConfig(true)))
              }
            }

            //分流曝光日志
            val displayArrayObj: JSONArray =
              jsonObject.getJSONArray("displays")
            if (displayArrayObj != null && displayArrayObj.size() > 0) {
              for (i <- 0 until displayArrayObj.size()) {
                val displayObj: JSONObject =
                  displayArrayObj.getJSONObject(i)
                val displayType: String =
                  displayObj.getString("display_type")
                val displayItem: String =
                  displayObj.getString("item")
                val displayItemType: String =
                  displayObj.getString("item_type")
                val displayOrder: String =
                  displayObj.getString("order")
                val displayPosId: String =
                  displayObj.getString("pos_id")
                //封装 Bean
                val displayLog = PageDisplayLog(mid, uid, ar, ch,
                  isNew, md, os, vc, pageId, lastPageId, pageItem, pageItemType,
                  duringTime, displayType, displayItem, displayItemType, displayOrder, displayPosId, ts)
                MyKafkaUtil.send(dwd_page_display, JSON.toJSONString(displayLog, new
                    SerializeConfig(true)))
              }


            }
            //分流启动日志
            val startObj: JSONObject =
              jsonObject.getJSONObject("start")
            if (startObj != null) {
              val entry: String = startObj.getString("entry")
              val loadingTimeMs: Long =
                startObj.getLong("loading_time_ms")
              val openAdId: String =
                startObj.getString("open_ad_id")
              val openAdMs: Long = startObj.getLong("open_ad_ms")
              val openAdSkipMs: Long =
                startObj.getLong("open_ad_skip_ms")
              //封装 Bean
              val startLog =
                StartLog(mid, uid, ar, ch, isNew, md, os, vc, entry, openAdId, loadingTimeMs, openAdMs, openAdSkipMs, ts)
              //发送 Kafka
              MyKafkaUtil.send(dwd_start_log, JSON.toJSONString(startLog, new
                  SerializeConfig(true)))


            }
          }
        }
        MyKafkaUtil.flush()
      })
      OffsetManagerUtil.saveOffset(ods_base_topic,group_id,offsetRanges)

    })
      sparkCnt.start()
      sparkCnt.awaitTermination()

}}
