import com.alibaba.fastjson.JSON;
import com.spark.kafka.bean.PageLog;
import com.spark.kafka.util.JedisUtil;
import redis.clients.jedis.Jedis;

import java.time.*;
import java.time.temporal.TemporalUnit;
import java.util.Date;
import java.util.Set;

public class Test {

    public static void main(String[] args) {
        //Jedis jedis = JedisUtil.getJedis();
//        Jedis jedis1=new Jedis("hadoop2",6379);
//
//        Set<String> keys = jedis1.keys("*");
//        System.out.println(keys);
        PageLog pageLog = new PageLog("1", "2", "1", "2", "1", "2",
                "1", "2", "1", "2", "1", "2",
                1, 2);
        System.out.println(JSON.toJSONString(pageLog));
        System.out.println(LocalDate.now());
        LocalDate now = LocalDate.now();
        LocalDateTime now1 = LocalDateTime.now();
        LocalDateTime of = LocalDateTime.of(2022, 6, 10, 12, 0, 0);
        System.out.println(of.getSecond());
        ZonedDateTime zonedDateTime = of.atZone(ZoneOffset.systemDefault());
        Date date = Date.from(zonedDateTime.toInstant());
        System.out.println(date);


        Duration between = Duration.between(now1,of);
        System.out.println(between.getSeconds());

//        System.out.println(now1);
//        Period period = Period.between(now, LocalDate.of(2022, 6, 10));
//        System.out.println(period.getDays());
    }
}
