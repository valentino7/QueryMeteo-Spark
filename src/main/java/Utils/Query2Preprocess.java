package Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;



public class Query2Preprocess {


    public static JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double,Double> > executeProcess(Map<String, Tuple2<String,String>> nations,JavaRDD<Tuple3<String,String,Double>> values, int fileType) {


         return values
                .mapToPair(new PairFunction<Tuple3<String, String, Double>, Tuple3<Integer, Integer, String>, Tuple2<Double, Double>>() {
                    @Override
                    public Tuple2<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>> call(Tuple3<String, String, Double> tuple) throws Exception {

                        // read date time in custom format
                        String datePattern = "yyyy-MM-dd HH:mm:ss";
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(datePattern);
                        LocalDateTime date = LocalDateTime.parse(tuple._1(), formatter);


                        // transform local date time in UTC format
                        ZoneId utcZone = ZoneOffset.UTC;
                        ZonedDateTime utcTime = ZonedDateTime.of(date,utcZone);

                        return new Tuple2<>(new Tuple3<>(utcTime.getYear(), utcTime.getMonth().getValue(), nations.get(tuple._2())._1() ), new Tuple2<>( ( (fileType == 0) & (tuple._3() > 400) ) ? tuple._3()/1000 : tuple._3() ,1.0)  );

                    }
                })
                 .filter(v1 -> v1._2._1() != 0.0)
                 .cache();
    }



}
