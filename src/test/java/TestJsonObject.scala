import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.spark.kafka.bean.PageLog



object TestJsonObject {
  def main(args: Array[String]): Unit = {

    val log = new PageLog("1","2","1","2","1","2",
      "1","2","1","2","1","2",
      1,2)
    val str = JSON.toJSONString(log, new SerializeConfig(true))
    val json = JSON.toJSON(log, new SerializeConfig(true))
    val nObject = json.asInstanceOf[JSONObject]
    println(nObject.getString("mid"))
    println()

  }
}
