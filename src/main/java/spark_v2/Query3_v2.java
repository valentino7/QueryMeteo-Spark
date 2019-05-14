package spark_v2;

import Utils.TupleComparator;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.*;

import java.lang.Boolean;
import java.lang.Double;
import java.util.*;


public class Query3_v2 {

    public static void executeQuery(JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> values){


        JavaPairRDD<Tuple2<Integer,String>, Iterable<String>> result = values
                .filter(new Function<Tuple2<Tuple5<Integer,Integer,Integer, String, String>, Tuple2<Double,Double > >, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Tuple5<Integer,Integer,Integer, String, String>, Tuple2<Double,Double >> v1) throws Exception {
                        if ( (v1._1()._1()>= 2016)&(v1._1()._3() >= 12  & v1._1()._3() <= 15) & (  (v1._1()._2() >= 6 & v1._1()._2() <= 9 ) || ( (v1._1()._2() >= 1 & v1._1()._2() <= 4) )  ) ){
                            return true;
                        }
                        else return false;
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
                .cache();


        JavaPairRDD<String, Iterable<String>> result2016 = result
                .filter( t -> t._1()._1() == 2016)
                .mapToPair( t -> new Tuple2<>(t._1()._2(),t._2()));


        JavaPairRDD<String, Iterable<String> > result2017 = result
                .filter( t -> t._1()._1() == 2017)
                .mapToPair( t -> new Tuple2<>(t._1()._2(),t._2()))
                .mapValues(new Function<Iterable<String>, Iterable<String>>() {
                    @Override
                    public Iterable<String> call(Iterable<String> v1) throws Exception {
                        Iterable<String> limit = Iterables.limit(v1, 3);
                        //System.out.println(limit);
                        return limit;
                    }
                });
        // mappo i mesi in 2 quadrimestri
        // (Anno,Quadrimestre,Nazione,Città) -> (value,count)
        // reduceByKey (Anno,Quadrimestre,Nazione,Città) -> (sumValues,sumCount)
        // mapValues (Anno,Quadrimestre,Nazione,Città) -> ( average = sumValues/sumCount )
        // map ( Anno,Nazione,Città) -> avg
        // reduceByKey (Anno,Nazione,Città) -> |avg1-avg2|


        /*Anno1 Nazione1 città1 valore1
                         città2 valore2
                         città3 valore3
                         città4 valore4
                         città5 valore5

                Nazione2 città1 valore1
                         città2 valore2
                         città3 valore3
                         città4 valore4
                         città5 valore5

          Anno2 Nazione1 città1 valore1
                         città2 valore2
                         città3 valore3
                         città4 valore4
                         città5 valore5

                Nazione2 città1 valore1
                         città2 valore2
                         città3 valore3
                         città4 valore4
                         città5 valore5

*/

        List<Tuple2<String,Iterable<String>>> map = result2017.collect();

        for ( Tuple2<String,Iterable<String>> s : map){
            System.out.println(s );
        }

        // result.saveAsTextFile("Query3result");
    }
}
