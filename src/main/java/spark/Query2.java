package spark;

import Utils.Constants;
import Utils.Context;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Query2 {

    public static void executeQuery(String[] args){

        JavaSparkContext sc=Context.getContext("Query2");


        //JavaRDD<String> city_attributes = sc.textFile(Constants.CITY_FILE);

        JavaRDD<String> attributes_file = sc.textFile(Constants.TEMPERATURE_FILE);
        String firstLine = attributes_file.first();
        List<String> citiesArray = new ArrayList<>(Arrays.asList(firstLine.split(",")));
        citiesArray.remove(0);




        //creazione file di citta-nazione in preprocessing attraverso le api
        //caricare il file come hashmap,quindi abbiamo una variabile hashmap del tipo citta-nazione
        //trovo la nazione data la citta
        JavaPairRDD<Tuple3<Integer,Integer,String>, Tuple2<Double, Double> > dataset = attributes_file
                .filter( csvLine -> !csvLine.equals(firstLine) )
                .flatMapToPair((PairFlatMapFunction<String, Tuple3<Integer, Integer, String>, Tuple2<Double,Double> >) s -> {
                    String[] strings = s.split(",", -1);
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = df.parse(strings[0]);
                    GregorianCalendar cal = new GregorianCalendar();
                    cal.setTime(date);
                    Double d = 0.0;
                    List<Tuple2<Tuple3<Integer,Integer,String>,Tuple2<Double,Double> > > list = new ArrayList<>();
                    for (int i = 1; i < strings.length; i++) {
                        if(strings[i].isEmpty())
                            d=0.0;
                        else
                            d=Double.valueOf(strings[i]);
                        if (d>= 350)
                            d=d/1000;
                        if (d>=1000000)
                            d=d/10000;
                        list.add( new Tuple2<>(new Tuple3<>(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), citiesArray.get(i-1) ), new Tuple2<>( d ,1.0) ) );
                    }
                    return list.iterator();

                })
                .filter((Function<Tuple2<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>>, Boolean>) v1 -> {
                    if(v1._2._1() == 0.0) {
                        return false;
                    } else
                        return true;
                });
        // Somme per stesso mese, città e anno
        // (Anno,Mese,Città) -> (Somma di temperature, count temperature )
        JavaPairRDD<Tuple3<Integer,Integer,String>, Tuple2<Double, Double> > sum_count = dataset.reduceByKey((tuple1, tuple2) -> new Tuple2<>(tuple1._1()+tuple2._1(), tuple1._2()+ tuple2._2()));

        JavaPairRDD<Tuple3<Integer,Integer,String>, Double > average = sum_count.mapValues((Function<Tuple2<Double, Double>, Double>) v1 -> v1._1()/v1._2());


        JavaPairRDD<Tuple3<Integer,Integer,String>, Tuple2<Double, Double> > min_max = dataset.reduceByKey(new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {
            @Override
            public Tuple2<Double, Double> call(Tuple2<Double, Double> v1, Tuple2<Double, Double> v2) throws Exception {
                Double max;
                Double min ;
                if (v1._1() >= v2._1()) {
                    max = v1._1();
                }else{
                    max = v2._1();
                }
                if (v1._1() <= v2._1()) {
                    min = v1._1();
                } else{
                    min  = v2._1();
                }

                return new Tuple2<>(max,min);
            }
        });

        Map<Tuple3<Integer,Integer,String>, Double >map = average.collectAsMap();
        Map<Tuple3<Integer,Integer,String>, Tuple2<Double, Double> >map1 = min_max.collectAsMap();
       /* for ( Tuple3<Integer,Integer,String> d : map.keySet()){
            System.out.println(d + " -> " + map.get(d) );
        }*/

        for ( Tuple3<Integer,Integer,String> d : map1.keySet()){
            System.out.println(d + " -> " + map1.get(d) );
        }

        //average.saveAsTextFile("output");

        sc.stop();
    }
}
