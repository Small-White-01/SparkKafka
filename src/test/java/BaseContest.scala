import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BaseContest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("asd").setMaster("local[*]")
    var sc:SparkContext =new SparkContext(conf);
    val value = sc.makeRDD(Array(1, 2, 3, 4), 2)
    //
    val value1: RDD[Array[Int]] = value.glom()
    value1.groupBy()
    value.foreach(println(_))
    value1.foreach(arr=>println(arr.mkString(",")))



//    value.distinct()
   // dataRDD.foreach(println(_))
  }

}
