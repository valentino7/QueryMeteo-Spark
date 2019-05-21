package sparkCore;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.*;

import java.lang.Boolean;
import java.lang.Double;
import java.util.*;


public class Query3 {

    public static JavaPairRDD<String, List<Tuple2<String,Integer> >>  executeQuery(JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> values){

        /*

        input :
               (Year,Month,Hour,City,Country), (Temp,Count)

        .filter:
                2016<=years<=2017
                1<=month<=4 & 6<=month<=9
                12<=hour<=15

        .mapToPair : remove hour from tuple in key and add indicator for 4 Month
                K : tuple5 (Year,Month,Hour,City,Country)  -> tuple4 (Year,4Month,City,Country)
                                                                     1<=Month<=4  ->  4Month = 1
                                                                     6<=Month<=9  ->  4Month = 2
                V : same (Temp,Count)

        .reduceByKey : calculate sum temperature and sum count for same (Year,4Month,Hour,City,Country)

         */

        JavaPairRDD<Tuple4<Integer, Integer, String, String>, Tuple2<Double, Double>> temp = values
                .filter(new Function<Tuple2<Tuple5<Integer,Integer,Integer, String, String>, Tuple2<Double,Double > >, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Tuple5<Integer,Integer,Integer, String, String>, Tuple2<Double,Double >> v1) throws Exception {
                        return (v1._1()._1() >= 2016) & (v1._1()._3() >= 12 & v1._1()._3() <= 15) & ((v1._1()._2() >= 6 & v1._1()._2() <= 9) || ((v1._1()._2() >= 1 & v1._1()._2() <= 4)));
                    }
                })
                .mapToPair(new PairFunction<Tuple2<Tuple5<Integer, Integer, Integer, String, String>, Tuple2<Double, Double>>, Tuple4<Integer, Integer, String, String>, Tuple2<Double, Double>>() {
                    @Override
                    public Tuple2<Tuple4<Integer, Integer, String, String>, Tuple2<Double, Double>> call(Tuple2<Tuple5<Integer, Integer, Integer, String, String>, Tuple2<Double, Double>> tuple) throws Exception {
                        return new Tuple2<> ( new Tuple4<> (tuple._1()._1(),
                                (tuple._1()._2() == 1 || tuple._1()._2() == 2 || tuple._1()._2() == 3 || tuple._1()._2() == 4 ) ? 1 : 2,
                                tuple._1()._4(), tuple._1()._5() ), tuple._2() );
                    }
                })
                .reduceByKey(new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {
                    @Override
                    public Tuple2<Double, Double> call(Tuple2<Double, Double> v1, Tuple2<Double, Double> v2) throws Exception {
                        return new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2());
                    }
                })
                .cache();


          /*

        .mapValues: calculate average temperature sumTemperature/sumCount


        .mapToPair : remove 4Month from tuple in key
                K : tuple4 (Year,4Month,City,Country)  -> tuple3 (Year,City,Country)
                V : same (Temp,Count)

        .reduceByKey : calculate mean temperature differenze (in module) for same (Year,City,Country)

         */




        // ( anno, nazione, citta) -> valore
        //( anno, nazione ) -> (Lista ordinata di città)
        JavaPairRDD<Tuple2<Integer, String>, Iterable<String>> result = temp
                .mapValues(new Function<Tuple2<Double, Double>, Double>() {
                    @Override
                    public Double call(Tuple2<Double, Double> v1) throws Exception {
                        return v1._1()/v1._2();
                    }
                })
                .mapToPair(new PairFunction<Tuple2<Tuple4<Integer, Integer, String, String>, Double>, Tuple3<Integer,String, String>, Double>() {
                    @Override
                    public Tuple2<Tuple3<Integer, String, String>, Double> call(Tuple2<Tuple4<Integer, Integer, String, String>, Double> tuple) throws Exception {
                        return new Tuple2<>( new Tuple3<>(tuple._1._1(),tuple._1()._3(),tuple._1()._4()), tuple._2());
                    }
                })
                .reduceByKey(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double v1, Double v2) throws Exception {
                        return Math.abs(v1-v2);
                    }
                })

                .mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))
                .sortByKey(Comparator.reverseOrder())
                .mapToPair(tuple -> new Tuple2<>(new Tuple2<>(tuple._2()._1(),tuple._2()._2()), tuple._2()._3()))
                .groupByKey()
                /* .mapToPair(tuple -> {
                     if (tuple._1._1 == 2017){
                         return new Tuple2<>(tuple._1._2(),new Tuple2<>(tuple._1._1(),Iterables.limit(tuple._2(),3)));
                     }
                     else {
                         return new Tuple2<>(tuple._1._2(), new Tuple2<>(tuple._1._1(), tuple._2()));
                     }
                 })
                 //.groupByKey()
                 .mapValues( value -> {
                     Rank ranking = new Rank();

                     Iterable<String> tempValue = value._2;
                     ranking.setYear(value._1());
                     List<String> r = new ArrayList<>();
                     while(tempValue.iterator().hasNext()) {
                         r.add(tempValue.iterator().next());
                     }
                     ranking.setCityRank(r);
                     return ranking;
                 })
                 .reduceByKey(new Function2<Rank, Rank, Rank>() {
                     @Override
                     public Rank call(Rank rank, Rank rank2) throws Exception {
                         return null;
                     }
                 })*/
                .cache();



        JavaPairRDD<String, Iterable<String>> result2016 = result
                .filter( t -> t._1()._1() == 2016)
                .mapToPair( t -> new Tuple2<>(t._1()._2(),t._2()));

        Map<String,Iterable<String>> rank2016 = result2016.collectAsMap();
        Map<String, List<String> > rank2016list = new HashMap<>();
        for ( String s : rank2016.keySet()){
            List<String> l = new ArrayList<>();
            for (String string : rank2016.get(s) ){
                l.add(string);
            }
            rank2016list.put(s,l);
        }


        return result
                .filter( t -> t._1()._1() == 2017)
                .mapToPair( t -> new Tuple2<>(t._1()._2(),t._2()))
                .mapValues(new Function<Iterable<String>, Iterable<String>>() {
                    @Override
                    public Iterable<String> call(Iterable<String> v1) throws Exception {
                        return Iterables.limit(v1,3);
                    }
                })
                .mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, List<Tuple2<String, Integer>>>() {
                    @Override
                    public Tuple2<String, List<Tuple2<String, Integer>>> call(Tuple2<String, Iterable<String>> v1) throws Exception {

                        List<Tuple2<String, Integer>> listToreturn = new ArrayList<>();
                        for (String s : v1._2()) {
                            List<String> temp = rank2016list.get(v1._1());
                            listToreturn.add(new Tuple2<>(s, temp.indexOf(s)+1));
                        }
                        return new Tuple2<>(v1._1(),listToreturn);
                    }
                })
                .cache();


        //GetTime
        /*JavaRDD<String> toJson = result2017
                .map(tuple -> new Gson().toJson(tuple));

        toJson.saveAsTextFile(Constants.HDFS_MONGO_QUERY3);*/

        //GetTime
    }
}
