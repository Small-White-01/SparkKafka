import com.spark.kafka.util.DateUtil
import org.apache.spark.sql.catalyst.util.DateFormatter

import java.text.SimpleDateFormat
import java.time.{Duration, LocalDate}
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date}

object LocalDateTest {

  def main(args: Array[String]): Unit = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    println(DateUtil.getLocalDate("1992-12-04 00:00:00"))

  }

}
