package com.spark.kafka.bean
case class OrderInfo(
var id: Long =0L,
var province_id: Long=0L,
var order_status: String=null,
var user_id: Long=0L,
var total_amount: Double=0D,
var activity_reduce_amount: Double=0D,
var coupon_reduce_amount: Double=0D,
var original_total_amount: Double=0D,
var feight_fee: Double=0D,
var feight_fee_reduce: Double=0D,
var expire_time: String =null,
var refundable_time:String =null,
var create_time: String=null,operate_time: String=null,
var create_date: String=null, // 把其他字段处 理得到
var create_hour: String=null,
  var province_name:String=null,//查询维度表得到
  var province_area_code:String=null,
  var province_3166_2_code:String=null,
  var province_iso_code:String=null,
  var user_age :Int=0, //查询维度表得到
  var user_gender:String=null
  ) {
  }