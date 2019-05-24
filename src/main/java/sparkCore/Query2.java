package sparkCore;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.*;

import java.lang.Double;


public class Query2 {



    public static JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple4<Double, Double ,Double, Double>> executeQuery(JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double,Double> > values){

        /*

        Input : RDD<(Year,Month,Nation), (value,count)> where
            value can be pressure,humidity or temperature in double format
            count : 1


        from filter dataset cached before, we calculate average

        .reduceByKey: sum values and count

        .mapValues : same key , (SumValues,SumCount) -> (average = SumValues/SumCount )

        .chache : for use in next operation

         */

        JavaPairRDD<Tuple3<Integer,Integer,String>,Double> average = values
                .reduceByKey((tuple1, tuple2) -> new Tuple2<>(tuple1._1()+tuple2._1(), tuple1._2()+ tuple2._2()))
                .mapValues((Function<Tuple2<Double, Double>, Double>) v1 -> v1._1()/v1._2())
                .cache();

        /*
          from filter dataset cached before, we calculate min and max

        .redeceByKey : compare first element of tuple for find max and min
                       store max in first element and min in second element of tuple

         */

        JavaPairRDD<Tuple3<Integer,Integer,String>, Double > max = values
                .mapValues(Tuple2::_1)
                .reduceByKey((Function2<Double, Double, Double>) Math::max)
                .cache();


        JavaPairRDD<Tuple3<Integer,Integer,String>, Double > min = values
                .mapValues(Tuple2::_1)
                .reduceByKey((Function2<Double, Double, Double>) Math::min)
                .cache();

        /*
          from filter dataset cached before, we calculate std

        .join: join on the same key, result : RDD<K,V> where
            K : (year,month,nation)
            V : ( (value,count), average for that key)

         .mapValues : same key , ( (value,count), average for that key) to ( (value - average)^2 , count )

         .reduceByKey : sum of (value - average)^2 and count

         .mapValues : same key , calculate sqrt ( (value - average)^2 / count-1  )

         */

        JavaPairRDD<Tuple3<Integer,Integer,String>, Double> std_dev = values
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
                })
                .cache();


        JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>> result_min_max = min.join(max).cache();
        JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>> result_avg_stddev = average.join(std_dev).cache();

        /*
        .join = aggregate all statistics with same key


         */


        return result_avg_stddev
                .join(result_min_max)
                .mapValues(value -> new Tuple4<>(value._1._1(),value._1._2,value._2._1(), value._2._2()));

    }
}

