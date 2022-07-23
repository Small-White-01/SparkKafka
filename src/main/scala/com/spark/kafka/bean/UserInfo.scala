package com.spark.kafka.bean

case class UserInfo(
                   var id:Long,
                   var birthday:String,
                   var gender:String
                   ){
  def this(){
    this(0,null,null)
  }
}
