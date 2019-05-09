package spark;

import Utils.Constants;
import Utils.Context;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Query2 {

    public static void executeQuery(String[] args){

        JavaSparkContext sc=Context.getContext("Query2");


        JavaRDD<String> city_attributes = sc.textFile(Constants.CITY_FILE);

        JavaRDD<String> attributes_file = sc.textFile(args[0]);
        String firstLine = attributes_file.first();


        JavaPairRDD<Tuple3<Integer,Integer,Integer>, Tuple2<Double, Integer> > dataset = attributes_file
                .flatMapToPair((PairFlatMapFunction<String, Tuple3<Integer, Integer, Integer>, Tuple2<Double,Integer> >) s -> {
                    String[] strings = s.split(",", -1);
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = df.parse(strings[0]);
                    GregorianCalendar cal = new GregorianCalendar();
                    cal.setTime(date);
                    List<Tuple2<Tuple3<Integer,Integer,Integer>,Tuple2<Double,Integer> > > list = new ArrayList<>();
                    for (int i = 1; i < strings.length; i++) {
                        list.add( new Tuple2<>(new Tuple3<>(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), i ), new Tuple2<>(Double.valueOf(strings[i]) ,1) ) );
                    }
                    return list.iterator();

                });

        sc.stop();
    }
}
