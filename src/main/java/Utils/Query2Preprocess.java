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


    // input (date,city,values)
    public static JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double,Double> > executeProcess(Map<String, Tuple2<String,String>> nations,JavaRDD<Tuple3<String,String,Double>> values, int fileType) {

/*
        .mapToPair :
                    return RDD<K,V> where:
                        K = ( year , month , city )
                        V = (value, count)

        .filter : remove null values
*/
         return values
                .mapToPair((PairFunction<Tuple3<String, String, Double>, Tuple3<Integer, Integer, String>, Tuple2<Double, Double>>) tuple -> {

                    ZonedDateTime dateTime = ConvertTime.convertTime(tuple._1(),nations.get(tuple._2())._2());

                    return new Tuple2<>(new Tuple3<>(dateTime.getYear(), dateTime.getMonth().getValue(), nations.get(tuple._2())._1() ), new Tuple2<>( ( (fileType == 0) & (tuple._3() > 400) ) ? tuple._3()/1000 : tuple._3() ,1.0)  );

                })
                 .filter(v1 -> v1._2._1() != 0.0)
                 .cache();
    }



}
