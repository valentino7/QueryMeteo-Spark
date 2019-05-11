package spark_v2;

import Utils.TupleComparator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.*;

import java.lang.Boolean;
import java.lang.Double;


public class Query3_v2 {

    public static void executeQuery(JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> values){


        JavaPairRDD<Tuple2<Integer,String>, Iterable<Tuple2<String, Double >>> result = values
                .filter(new Function<Tuple2<Tuple5<Integer,Integer,Integer, String, String>, Tuple2<Double,Double > >, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Tuple5<Integer,Integer,Integer, String, String>, Tuple2<Double,Double >> v1) throws Exception {
                        if ( (v1._1()._3() >= 12  & v1._1()._3() <= 15) & (  (v1._1()._2() >= 6 & v1._1()._2() <= 9 ) || ( (v1._1()._2() >= 1 & v1._1()._2() <= 4) )  ) ){
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
                .mapToPair(tuple -> new Tuple2<>( new Tuple2<>(tuple._1()._3(),tuple._2()),new Tuple2<>(tuple._1()._1(),tuple._1()._2())))
                .sortByKey(new TupleComparator())
                .mapToPair(tuple -> new Tuple2<>(tuple._2(),tuple._1()))
                .groupByKey();


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

        /*Map<Tuple2<Integer,String>, Iterable<Tuple2<String,Double>> > map = result.collectAsMap();
        for (Tuple2<Integer,String> t : map.keySet()){
            System.out.println(t +"\t" + map.get(t));
        }*/
        result.saveAsTextFile("Query3result");
    }
}
