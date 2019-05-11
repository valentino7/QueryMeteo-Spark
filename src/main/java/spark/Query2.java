package spark;

import Utils.Constants;
import Utils.Context;
import Utils.PreProcess;
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


        JavaRDD<String> attributes_file = sc.textFile(Constants.HUMIDITY_FILE);
        String firstLine = attributes_file.first();
        List<String> citiesArray = new ArrayList<>(Arrays.asList(firstLine.split(",")));
        citiesArray.remove(0);

        // hashmap <Città,(Nazione,TimeZoneID)>
        Map<String,Tuple2<String,String>> city_nations = PreProcess.executeProcess(sc);

        System.exit(0);

        //(Anno,Mese,Nazione) -> ( value, count )

        JavaPairRDD<Tuple3<Integer,Integer,String>, Tuple2<Double, Double> > dataset = attributes_file
                .filter( csvLine -> !csvLine.equals(firstLine) )
                .flatMapToPair((PairFlatMapFunction<String, Tuple3<Integer, Integer, String>, Tuple2<Double,Double> >) s -> {
                    String[] strings = s.split(",", -1);
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = df.parse(strings[0]);
                    GregorianCalendar cal = new GregorianCalendar();
                    cal.setTime(date);
                    double d = 0.0;
                    List<Tuple2<Tuple3<Integer,Integer,String>,Tuple2<Double,Double> > > list = new ArrayList<>();
                    for (int i = 1; i < strings.length; i++) {
                        if(strings[i].isEmpty())
                            d=0.0;
                        else
                            d= Double.parseDouble(strings[i]);
                      /*  if (d>= 350)
                            d=d/1000;
                        if (d>=1000000)
                            d=d/10000;*/
                        list.add( new Tuple2<>(new Tuple3<>(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), city_nations.get(citiesArray.get(i-1))._1() ), new Tuple2<>( d ,1.0) ) );
                    }
                    return list.iterator();
                })
                .filter((Function<Tuple2<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>>, Boolean>) v1 -> v1._2._1() != 0.0)
                .cache();

        JavaPairRDD<Tuple3<Integer,Integer,String>, Tuple2<Double, Double> > sum_count = dataset.reduceByKey((tuple1, tuple2) -> new Tuple2<>(tuple1._1()+tuple2._1(), tuple1._2()+ tuple2._2()));

        JavaPairRDD<Tuple3<Integer,Integer,String>, Double > average = sum_count
                .mapValues((Function<Tuple2<Double, Double>, Double>) v1 -> v1._1()/v1._2())
                .cache();


        JavaPairRDD<Tuple3<Integer,Integer,String>, Tuple2<Double, Double> > min_max = dataset
                .reduceByKey(new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {
                    @Override
                    public Tuple2<Double, Double> call(Tuple2<Double, Double> v1, Tuple2<Double, Double> v2) throws Exception {
                        Double max;
                        Double min ;
                        if (v1._1() > v2._1()) {
                            max = v1._1();
                        }else{
                            max = v2._1();
                        }
                        if (v1._1() < v2._1()) {
                            min = v1._1();
                        } else{
                            min  = v2._1();
                        }
                        return new Tuple2<>(max,min);
                    }
                });

        JavaPairRDD<Tuple3<Integer,Integer,String>, Double> std_dev = dataset
                .join(average)
                .mapValues(new Function<Tuple2<Tuple2<Double, Double>, Double>, Tuple2<Double,Double>>() {
                    @Override
                    public Tuple2<Double,Double> call(Tuple2<Tuple2<Double, Double>, Double> v1) throws Exception {
                        return new Tuple2<>( Math.pow(v1._1()._1() - v1._2(),2) , v1._1()._2());
                    }
                })
                .reduceByKey(new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {
                    @Override
                    public Tuple2<Double, Double> call(Tuple2<Double, Double> v1, Tuple2<Double, Double> v2) throws Exception {

                        return new Tuple2<>( v1._1() +  v2._1(), v1._2() +v2._2());
                    }
                })
                .mapValues(new Function<Tuple2<Double, Double>, Double>() {
                    @Override
                    public Double call(Tuple2<Double, Double> v1) throws Exception {
                        return Math.sqrt(v1._1()/ (v1._2()-1));
                    }
                });


     /*  Map < Tuple3 < Integer, Integer, String >, Double > avergeMap = average.collectAsMap();
       Map<Tuple3<Integer,Integer,String>, Tuple2<Double, Double> >min_maxMap = min_max.collectAsMap();


        for ( Tuple3<Integer,Integer,String> d : avergeMap.keySet()){
            System.out.println(d + " -> " + avergeMap.get(d) );
        }

        System.out.println("----------------------------------------------------------------------");

        for ( Tuple3<Integer,Integer,String> d : min_maxMap.keySet()){
            System.out.println(d + " -> " + min_maxMap.get(d) );
        }

        System.out.println("----------------------------------------------------------------------");

        //Map<Tuple3<Integer,Integer,String>, Double > stdMap = std_dev.collectAsMap();
     /*   for ( Tuple3<Integer,Integer,String> d : stdMap.keySet()){
            System.out.println(d + " -> " + stdMap.get(d) );
        }
*/
        //  std_dev.saveAsTextFile("output");
        min_max.saveAsTextFile("minMaxHumidity");
        average.saveAsTextFile("averageHumidity");
        std_dev.saveAsTextFile("stdHumidity");

        sc.stop();
    }
}
