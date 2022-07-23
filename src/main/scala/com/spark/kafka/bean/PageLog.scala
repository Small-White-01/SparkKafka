package com.spark.kafka.bean

case class PageLog(
var mid :String,
var user_id:String,
var province_area_code:String,
var channel:String,
var is_new:String,
var model:String,
var operate_system:String,
var version_code:String,
var page_id:String ,
var last_page_id:String,
var page_item:String,
var page_item_type:String,
var during_time:Long,
var ts:Long
) {
}