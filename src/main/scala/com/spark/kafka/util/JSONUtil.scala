package com.spark.kafka.util

import com.alibaba.fastjson.JSON
import com.fasterxml.jackson.databind.exc.InvalidFormatException
import com.spark.kafka.bean.OrderDetail

object JSONUtil {


  def validate(json:String):Boolean={
    try{
      JSON.parseObject(json)
      return true
    }catch {
      case e:InvalidFormatException=>return false
    }
  }


  def main(args: Array[String]): Unit = {
    var json:String="{\"sku_num\":\"2\",\"create_time\":\"2020-12-22 23:15:04\",\"sku_id\":6,\"order_price\":1299.00,\"source_type\":\"2401\",\"img_url\":\"http://47.93.148.192:8080/group1/M00/00/01/rBHu8l-rgJqAHPnoAAF9hoDNfsc505.jpg\",\"sku_name\":\"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米\",\"id\":107222,\"order_id\":35660,\"split_total_amount\":2598.00}"
    validate(json)
    val detail = JSON.parseObject(json, classOf[OrderDetail])
    println(detail)
  }

}
