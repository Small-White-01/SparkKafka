package com.spark.kafka.util

import java.util.ResourceBundle

object PropertiesUtil {
  val resource:ResourceBundle=ResourceBundle.getBundle("config")


  def getString(key:String):String={
    val str = resource.getString(key)
    str
  }

  def main(args: Array[String]): Unit = {
    println(getString("kafka.broker_list.url"))
  }

}
