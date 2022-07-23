import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

object JoinDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("odsBaseDb")
    var sc=new SparkContext(sparkConf)


    val value = sc.makeRDD(Array((1, "aaa"), (2, "bbb")))

    value.collect()
    val value2 = sc.makeRDD(Array((1, "1"), (2, "2")))

  }

}
