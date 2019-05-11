package spark_v2;

import Utils.Constants;
import Utils.Context;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class Query1_v2 {


    public static void executeQuery(JavaRDD<Tuple3<String,String,Double>> values){

        /*
        .filter : Remove Header

        .flatMapToPair :
            RDD<String> to RDD<K,V> where:
                K = ( year , month , day , city )
                V = 0 or 1
                    1 : description was "sky is clear"
                    0 : other

        .filter :
            RDD<(year,month,day,city), value > on Month: March.April,May

        .reduceByKey :
            Sum values with same day:
            In this way we calculate the number of "sky is clear" in a day

        .mapToPair :
             RDD<(year,month,day,city), count> to RDD<K,V> where:
                K : (year,month,city)
                V : 0 or 1
                    1 : count > N
                        N is the number of "sky is clear" needed to consider "clear" a day
                    0 : else
        .reduceByKey :
            Sum values with same month:
            In this way we calculate the number of clear days in a month

        .filter :
            RDD<(year,month,city), count>  on count: take the Month with at least 15 clear day

        .mapToPair :
            RDD<(year,month,city), count>  to RDD< K,V> where :
                K = year/month
                V = cities

        .groupByKey : group by year/month
        .sortByKey : order by year/month
         */

        //  List<Tuple2<Tuple4<Integer,Integer,Integer,String>,Integer>> list = new ArrayList<>();




        JavaPairRDD<Integer, Iterable<String>> citiesWithClearSky = values
                .mapToPair(new PairFunction<Tuple3<String, String, Double>, Tuple4<Integer,Integer,Integer,String>, Double>() {
                    @Override
                    public Tuple2<Tuple4<Integer, Integer, Integer, String>, Double> call(Tuple3<String, String, Double> tuple) throws Exception {
                        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = df.parse(tuple._1());
                        GregorianCalendar cal = new GregorianCalendar();
                        cal.setTime(date);

                        return new Tuple2<>(new Tuple4<>(cal.get(Calendar.YEAR),cal.get(Calendar.MONTH),cal.get(Calendar.DAY_OF_MONTH),tuple._2()),tuple._3());
                    }
                })
                .filter( object -> (
                        object._1._2() == 2 ||
                                object._1._2() == 3 ||
                                object._1._2() == 4 ) )
                .reduceByKey(Double::sum)
                .mapToPair((PairFunction<Tuple2<Tuple4<Integer, Integer, Integer, String>, Double>, Tuple3<Integer, Integer, String>, Integer>) tuple -> {
                    if (tuple._2 >= 14.0){ //se almeno 12 ore in un giorno sono serene
                        return new Tuple2<>(new Tuple3<>(tuple._1._1(),tuple._1._2(),tuple._1._4()), 1);
                    }else {
                        return new Tuple2<>(new Tuple3<>(tuple._1._1(),tuple._1._2(),tuple._1._4()), 0);
                    }
                })
                .reduceByKey(Integer::sum)
                .filter((Function<Tuple2<Tuple3<Integer, Integer, String>, Integer>, Boolean>) tuple -> {
                    // città con più di 15 giorni al mese con tempo sereno
                    return tuple._2 >= 15;
                })
                .mapToPair((PairFunction<Tuple2<Tuple3<Integer, Integer, String>, Integer>, Tuple2<Integer,String>,Integer>) tuple -> {
                    return new Tuple2<>(new Tuple2<>(tuple._1()._1(),tuple._1._3()),1);
                })
                .reduceByKey(Integer::sum)
                .filter(v1 -> v1._2()>=3)
                .mapToPair( tuple ->  tuple._1())
                .groupByKey()
                .sortByKey();

        Map<Integer, Iterable<String>> map = citiesWithClearSky.collectAsMap();

        for( int x : map.keySet()) {
            System.out.println(x + "  " + map.get(x));
        }

        citiesWithClearSky.saveAsTextFile("clearSky");


    }


}

