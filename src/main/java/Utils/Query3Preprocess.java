package Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;

public class Query3Preprocess {

    // input (date,city,values)

    // output (year,month,hour,nation,city )
    public static JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> executeProcess(JavaSparkContext sc, JavaRDD<Tuple3<String,String,Double>> values) {


        Map<String, Tuple2<String,String>> nations = Nations.retryNation(sc);

        return values
                .mapToPair(new PairFunction<Tuple3<String, String, Double>, Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double, Double>>() {
                    @Override
                    public Tuple2<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double, Double>> call(Tuple3<String, String, Double> tuple) throws Exception {

                        // trasformare ora in ora locale tramite la timezone
                        String datePattern = "yyyy-MM-dd HH:mm:ss";
                        LocalDateTime ldt = LocalDateTime.parse(tuple._1(),  DateTimeFormatter.ofPattern(datePattern) );

                        ZoneId customZoneID = ZoneId.of(nations.get(tuple._2())._2);
                        ZonedDateTime localDateTime = ldt.atZone(customZoneID);


                        return new Tuple2<>(new Tuple5<>(localDateTime.getYear(), localDateTime.getMonth().getValue(), localDateTime.getHour(),nations.get(tuple._2())._1() ,tuple._2() ), new Tuple2<>(  (tuple._3() > 400)  ? tuple._3()/1000 : tuple._3() ,1.0)  );

                    }
                });
    }
}
