package spark_v2;


import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;


public class Query2_v2 {

    public static void executeQuery(JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double,Double> > values , int fileType){


        //(Anno,Mese,Nazione) -> ( value, count )

        /*

        Input : RDD<(Year,Month,Nation), (value,count)> where
            value can be pressure,humidity or temperature in double format
            count : 1

        .filter: remove null values

        .chache : for use in next operation

         */

        JavaPairRDD<Tuple3<Integer,Integer,String>, Tuple2<Double, Double> > dataset = values
                .filter((Function<Tuple2<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>>, Boolean>) v1 -> v1._2._1() != 0.0)
                .cache();

        /*

        from filter dataset cached before, we calculate average

        .reduceByKey: sum values and count

        .mapValues : same key , (SumValues,SumCount) -> (average = SumValues/SumCount )

        .chache : for use in next operation

         */

        // (anno,mese,nazione)
        JavaPairRDD<Tuple3<Integer,Integer,String>,Double> average = dataset
                .reduceByKey((tuple1, tuple2) -> new Tuple2<>(tuple1._1()+tuple2._1(), tuple1._2()+ tuple2._2()))
                .mapValues((Function<Tuple2<Double, Double>, Double>) v1 -> v1._1()/v1._2())
                .cache();

        /*
          from filter dataset cached before, we calculate min and max

        .redeceByKey : compare first element of tuple for find max and min
                       store max in first element and min in second element of tuple

         */

        JavaPairRDD<Tuple3<Integer,Integer,String>, Double > max = dataset
                .mapValues(Tuple2::_1)
                .reduceByKey((Function2<Double, Double, Double>) Math::max);

        JavaPairRDD<Tuple3<Integer,Integer,String>, Double > min = dataset
                .mapValues(Tuple2::_1)
                .reduceByKey((Function2<Double, Double, Double>) Math::min);
        /*
          from filter dataset cached before, we calculate std

        .join: join on the same key, result : RDD<K,V> where
            K : (year,month,nation)
            V : ( (value,count), average for that key)

         .mapValues : same key , ( (value,count), average for that key) to ( (value - average)^2 , count )

         .reduceByKey : sum of (value - average)^2 and count

         .mapValues : same key , calculate sqrt ( (value - average)^2 / count-1  )

         */

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




        JavaPairRDD<String, Iterable<Tuple5<String, Iterable<Double>, Iterable<Double>, Iterable<Double>, Iterable<Double>>>> aggregate = average
                .cogroup(max,min,std_dev)
                .mapToPair(new PairFunction<Tuple2<Tuple3<Integer, Integer, String>, Tuple4<Iterable<Double>, Iterable<Double>, Iterable<Double>, Iterable<Double>>>, String, Tuple5<String, Iterable<Double>, Iterable<Double>, Iterable<Double>, Iterable<Double>>>() {
                    @Override
                    public Tuple2<String, Tuple5<String, Iterable<Double>, Iterable<Double>, Iterable<Double>, Iterable<Double>>> call(Tuple2<Tuple3<Integer, Integer, String>, Tuple4<Iterable<Double>, Iterable<Double>, Iterable<Double>, Iterable<Double>>> tuple) throws Exception {
                        String data = tuple._1()._1().toString() + tuple._1()._2().toString();
                        return new Tuple2<>(tuple._1()._3(), new Tuple5<>(data, tuple._2._1(),tuple._2()._2(),tuple._2()._3(),tuple._2()._4()));
                    }
                })
                .groupByKey();




         /*JavaRDD<JsonObject> toJson = aggregate
                .map(new Function<Tuple2<String, Iterable<Tuple5<Integer, Iterable<Double>, Iterable<Double>, Iterable<Double>, Iterable<Double>>>>, JsonObject>() {
                         @Override
                         public JsonObject call(Tuple2<String, Iterable<Tuple5<String, Iterable<Double>, Iterable<Double>, Iterable<Double>, Iterable<Double>>>> v) throws Exception {
                             JsonObject doc = new JsonObject();
                             JsonArray statArrat = new JsonArray();
                             while ( v._2().iterator().hasNext() ){
                                 Tuple5<String, Iterable<Double>, Iterable<Double>, Iterable<Double>, Iterable<Double>> tupleTemp = v._2().iterator().next();
                             }
                             statArrat.
                             String month = new Gson().toJson("");
                             String monthProperties = new Gson().toJson(v1._2() );
                             doc.addProperty("country",v1._1().toString());
                             doc.addProperty("month", month);
                             return doc;
                             return null;
                         }
                     });*/

        aggregate.saveAsTextFile("results/query2/" + fileType);

    }
}

