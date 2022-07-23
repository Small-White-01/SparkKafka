package com.spark.kafka.util

import com.spark.kafka.bean.{DauInfo, PageLog}


import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks
object MyBeanUtil {

  def copyProperties(srcObj:AnyRef,desObj:AnyRef): Unit ={
    if(srcObj == null || desObj == null ){
      return
    }
    val fields:Array[Field] = srcObj.getClass.getDeclaredFields
    for (field <-fields){
      //内部就是continue，for外部就是break
      Breaks.breakable {
        field.setAccessible(true)
        //getter省略，与字段获取方法一样
        val getMethodName = field.getName
        val setMethodName = field.getName
        val value = field.get(srcObj)

//        val srcMethod = srcObj.getClass.getDeclaredMethod(getMethodName)
//        val desMethod: Method = try {
//          desObj.getClass.getDeclaredMethod(setMethodName)
//        } catch {
//          case e: NoSuchElementException => e.printStackTrace()
//          Breaks.break()
//        }
         val desField ={
            try{
                desObj.getClass.getDeclaredField(field.getName)
            }catch {
                case e:NoSuchFieldException=>
                Breaks.break()
            }
          }

        if(desField.getModifiers.equals(Modifier.FINAL)){
          Breaks.break()

        }
        desField.setAccessible(true)
        desField.set(desObj,value)

      }
    }

  }

  def main(args: Array[String]): Unit = {

    var log = new PageLog("1","2","3","4","5",
      "1","2","3","4","5",
      "1","2",3,3)
    var dauInfo = new DauInfo()


    copyProperties(log,dauInfo)
    println(dauInfo)

  }

}
