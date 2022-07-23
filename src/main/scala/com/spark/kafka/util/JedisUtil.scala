package com.spark.kafka.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisUtil {

  val jedisPool:JedisPool={
    val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
    val pool = new JedisPool(jedisPoolConfig,"hadoop2",
      6379)
    pool
  }
  def getJedis(): Jedis={
      jedisPool.getResource
  }

  def getIfNullFromMysql[U](jedis: Jedis,key_prefix:String,clazz:Class[U],col_value:Any,column:String):String ={
    var key=s"$key_prefix:$col_value"
    val json = jedis.get(key)
    if(json!=null&&json.nonEmpty){
      return json
    }
    //查询mysql
    val tableName = key_prefix.split(":")(1).toLowerCase
    val obj:U = DBUtil.queryFromId(clazz, tableName, col_value,column)

    if(obj!=null) {
      val jsonStr = JSON.toJSONString(obj, new SerializeConfig(true))
      jedis.set(key, jsonStr)
      jedis.close()
      jsonStr
    }else{
      jedis.close()
      throw new Exception(s"key:$key not found relative data in mysql")
    }


  }



}
