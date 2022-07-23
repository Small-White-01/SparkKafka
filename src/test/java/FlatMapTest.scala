import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object FlatMapTest {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("odsBaseDb")
    val ssc = new StreamingContext(sparkConf, Durations.seconds(5))




    ssc.start()
    ssc.awaitTermination()

  }
}
