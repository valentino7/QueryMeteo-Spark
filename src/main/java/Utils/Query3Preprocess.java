package Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class Query3Preprocess {

    // input (date,city,values)
    // output (year,month,hour,nation,city )
    public static JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> executeProcess(Map<String, Tuple2<String,String>> nations,JavaRDD<Tuple3<String,String,Double>> values) {

/*
        .mapToPair :return RDD<K,V> where:
                        K = ( year , month , hour , nation ,city )
                        V = ( temperature, count )

        .filter : remove null values
*/

        return values
                .mapToPair((PairFunction<Tuple3<String, String, Double>, Tuple5<Integer, Integer, Integer, String, String>, Tuple2<Double, Double>>) tuple -> {
                    ZonedDateTime dateTime = ConvertTime.convertTime(tuple._1(),nations.get(tuple._2())._2());
                    return new Tuple2<>(new Tuple5<>(dateTime.getYear(), dateTime.getMonth().getValue(), dateTime.getHour(),nations.get(tuple._2())._1() ,tuple._2() ), new Tuple2<>(  (tuple._3() > 400)  ? tuple._3()/1000 : tuple._3() ,1.0)  );

                })
                .filter(v1 -> v1._2._1() != 0.0)
                .cache();
    }
}
