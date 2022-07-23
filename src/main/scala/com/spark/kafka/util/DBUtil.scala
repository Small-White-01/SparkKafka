package com.spark.kafka.util

import com.spark.kafka.bean.{BaseProvince, UserInfo}

import java.lang.reflect.Field
import java.sql.{Connection, DriverManager, ResultSet}
import scala.collection.mutable.ListBuffer

object DBUtil {
{
    init()
}

  def init():Unit={
    Class.forName("com.mysql.jdbc.Driver")

  }

  def queryFromId[U](clazz: Class[U],tableName:String,id:Any,column:String): U= {

    var connection:Connection=  DriverManager
      .getConnection("jdbc:mysql://hadoop2:3306/gmallflink?useSSL=false","root","root")
    val fields:Array[Field] = clazz.getDeclaredFields
    val fieldNames:Array[String]=new Array[String](fields.length)
    for (i <-fields.indices){
      fieldNames(i)=fields(i).getName
    }
    val field_join = fieldNames.mkString(",")
    var sql = s"select $field_join from $tableName where $column=?"
    val preparedStatement = connection.prepareStatement(sql)


    preparedStatement.setObject(1, id)

    var resultSet:ResultSet=null
    val listBuffer = new ListBuffer[U]
    try {
       resultSet= preparedStatement.executeQuery()

      while (resultSet.next()) {
        val obj = clazz.newInstance()

        val metadata = resultSet.getMetaData

        for (i <- 0 until (metadata.getColumnCount)) {
          val column = metadata.getColumnLabel(i + 1)

          val field = {
            try {
              clazz.getDeclaredField(column)
            } catch {
              case e: Exception => null
            }
          }
          if (field != null) {
            field.setAccessible(true)
            field.set(obj, resultSet.getObject(column))
          }
        }
        listBuffer.append(obj)
      }
    }catch {
      case e:Exception=>null
    }finally {
      resultSet.close()
      preparedStatement.close()
      connection.close()
    }
    listBuffer.head
  }

  def main(args: Array[String]): Unit = {
//    val province = queryFromId(classOf[BaseProvince], "base_province", 650000,"area_code")
//    println(province)
    val userInfo = queryFromId(classOf[UserInfo], "user_info", 3000, "id")
    println(userInfo)
  }



}
