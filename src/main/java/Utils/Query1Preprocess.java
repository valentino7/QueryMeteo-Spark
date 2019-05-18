package Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Query1Preprocess {

    public static JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> executeProcess(Map<String, Tuple2<String,String>> nations,JavaRDD<Tuple3<String,String,Double>> values) {

        /*
        .mapToPair :return RDD<K,V> where:
                        K = ( year , month , day , city )
                        V = 0 or 1
                            1 : description was "sky is clear"
                            0 : other

        .filter :
             RDD<(year,month,day,city), value > on Month: March.April,May

*/
        return values
                .mapToPair(new PairFunction<Tuple3<String, String, Double>, Tuple4<Integer,Integer,Integer,String>, Double>() {
                    @Override
                    public Tuple2<Tuple4<Integer, Integer, Integer, String>, Double> call(Tuple3<String, String, Double> tuple) throws Exception {
                        // read date time in custom format
                        String datePattern = "yyyy-MM-dd HH:mm:ss";
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(datePattern);
                        LocalDateTime date = LocalDateTime.parse(tuple._1(), formatter);


                        // transform local date time in UTC format
                        ZoneId utcZone = ZoneOffset.UTC;
                        ZonedDateTime utcTime = ZonedDateTime.of(date,utcZone);

                        // convert UTC datetime in ZoneID datetime

                        ZonedDateTime dateTime = utcTime.withZoneSameInstant(ZoneId.of(nations.get(tuple._2())._2()));

                        return new Tuple2<>(new Tuple4<>(dateTime.getYear(),dateTime.getMonth().getValue(),dateTime.getDayOfMonth(),tuple._2()),tuple._3());
                    }
                })
                .filter( object -> (
                        object._1()._2() == 2 ||
                                object._1()._2() == 3 ||
                                object._1()._2() == 4 ) );
    }

}
