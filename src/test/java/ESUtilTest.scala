import com.spark.kafka.util.EsUtil

object ESUtilTest {
  def main(args: Array[String]): Unit = {

    EsUtil.search("gmall_dau_info_2022-06-09","mid")
  }
}
