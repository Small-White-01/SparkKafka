import com.spark.kafka.util.MyKafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

public class KafkaTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaProducer<String, String> kafkaProducer = MyKafkaUtil.createKafkaProducer();
        kafkaProducer.send(new ProducerRecord<String,String>("DWD_USER_INFO_I",0,"1",
                "test")).get();
    }
}
