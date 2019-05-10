package spark;

import Utils.Constants;
import Utils.Context;
import Utils.PreProcess;
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

public class Query3 {

    public static void executeQuery(String[] args){


        JavaSparkContext sc= Context.getContext("Query3");

        JavaRDD<String> attributes_file = sc.textFile(Constants.TEMPERATURE_FILE);
        String firstLine = attributes_file.first();
        List<String> citiesArray = new ArrayList<>(Arrays.asList(firstLine.split(",")));
        citiesArray.remove(0);

        // hashmap <Città,Nazione>
        Map<String,String> city_nations = PreProcess.executeProcess(sc);

        // convertire tutte le date nell'orario locale

        JavaPairRDD<Tuple4<Integer,Integer,String,String>, Tuple2<Double, Double>> dataset = attributes_file
                .filter( csvLine -> !csvLine.equals(firstLine) )
                .flatMapToPair((PairFlatMapFunction<String, Tuple4<Integer, Integer, String, String>, Tuple2<Double,Double> >) s -> {
                    String[] strings = s.split(",", -1);
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = df.parse(strings[0]);
                    GregorianCalendar cal = new GregorianCalendar();
                    cal.setTime(date);
                    double d = 0.0;
                    List<Tuple2<Tuple4<Integer,Integer,String,String>,Tuple2<Double,Double> > > list = new ArrayList<>();
                    for (int i = 1; i < strings.length; i++) {
                        if(strings[i].isEmpty())
                            d=0.0;
                        else
                            d= Double.parseDouble(strings[i]);
                        if (d>= 350)
                            d=d/1000;
                        if (d>=1000000)
                            d=d/10000;
                        list.add( new Tuple2<>(new Tuple4<>(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), city_nations.get(citiesArray.get(i-1)),citiesArray.get(i-1) ), new Tuple2<>( d ,1.0) ) );
                    }
                    return list.iterator();
                });

        sc.stop();
    }
}
