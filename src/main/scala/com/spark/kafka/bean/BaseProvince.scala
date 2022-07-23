package com.spark.kafka.bean

case class BaseProvince(
                     var id:Long,
                     var name:String,
                     var region_id:String,
                     var area_code:String,
                     var iso_code:String,
                     var iso_3166_2:String) {
  def this(){
    this(0,null,null,null,null,null)
  }
}
