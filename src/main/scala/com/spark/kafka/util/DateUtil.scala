package com.spark.kafka.util

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

object DateUtil{

  val date_formatter:DateTimeFormatter=DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val date_time_formatter:DateTimeFormatter=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")


  def getLocalDate(time:String):LocalDate={
    val strings = time.split(" ")
    val date = strings(0)
    LocalDate.parse(date,date_formatter)
  }
  def getLocalDateTime(time:String):LocalDateTime={
    LocalDateTime.parse(time,date_time_formatter)
  }

  def getTs(time:String):Long={
    val time1 = LocalDateTime.parse(time, date_time_formatter)
    time1.toInstant(ZoneOffset.of("+8")).toEpochMilli
  }

  def main(args: Array[String]): Unit = {
    val date = LocalDate.parse("2020-06-10 12:12:12")
    println(date)
  }
}
