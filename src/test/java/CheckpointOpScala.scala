import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CheckpointOpScala {

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setAppName("CheckpointOpScala")
      .setMaster("spark://hadoop2:7077")
//        .set("yarn.resourcemanager.hostname","hadoop3")
//        .set("spark.executor.instance","2")
//        .set("spark.executor.memory", "1024M")
//        .set("spark.driver.host","localhost")
//        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(conf)
      if(args.length==0){
        System.exit(100)
      }

      val outputPath = args(0)

      //1：设置checkpoint目录
      sc.setCheckpointDir("hdfs://hadoop2:8020/sparkCk")

      val dataRDD = sc.textFile("hdfs://hadoop2:8020/origin/README.txt")
        .persist(StorageLevel.DISK_ONLY)//执行持久化

      //2：对rdd执行checkpoint操作
      dataRDD.checkpoint()

      dataRDD.flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_ + _)
        .saveAsTextFile(outputPath)

      sc.stop()
    }
}



