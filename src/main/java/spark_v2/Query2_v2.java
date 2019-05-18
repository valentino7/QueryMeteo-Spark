package spark_v2;


import Utils.Constants;
import com.google.gson.Gson;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.*;

import java.lang.Boolean;
import java.lang.Double;



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
                .reduceByKey((Function2<Double, Double, Double>) Math::max)
                .cache();


        JavaPairRDD<Tuple3<Integer,Integer,String>, Double > min = dataset
                .mapValues(Tuple2::_1)
                .reduceByKey((Function2<Double, Double, Double>) Math::min)
                .cache();


        JavaPairRDD<String, Iterable<Tuple2<Integer, Iterable<Tuple3<Integer, Double, Double>>>>> kkk = min
                .join(max)
                .mapToPair(t -> new Tuple2<>(new Tuple2<>(t._1._1(),t._1._3()),new Tuple3<>(t._1._2(),t._2._1(),t._2._2())))
                .groupByKey()
                .mapToPair(t-> new Tuple2<>(t._1._2(),new Tuple2<>(t._1._1(),t._2())))
                .groupByKey();

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


        JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>> result_min_max = min.join(max);
        JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>> result_avg_stddev = average.join(std_dev);

        JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple4<Double, Double ,Double, Double>> result = result_avg_stddev
                .join(result_min_max)
                .mapValues(value -> new Tuple4<>(value._1._1(),value._1._2,value._2._1(), value._2._2()));

        JavaPairRDD<String, Iterable<Tuple2<Integer, Iterable<Tuple2<Integer, Tuple4<Double, Double, Double, Double>>>>>> record = result
                .mapToPair( t -> new Tuple2<>( new Tuple2<>(t._1._1(),t._1._3()), new Tuple2<>(t._1._2(),t._2()) ) )
                .groupByKey()
               /* .mapValues( value -> {
                    HashMap<Integer,Statistics> map = new HashMap();
                    value.forEach(tuple -> {
                        Statistics stat = new Statistics(tuple._2._1(), tuple._2._2(), tuple._2._3(), tuple._2._4());
                        map.put(tuple._1(),stat);
                    });
                    return map;
                })*/
                .mapToPair(t -> new Tuple2<>(t._1._2(), new Tuple2<>(t._1._1(),t._2)))
                .groupByKey();

        //System.out.println("scrittura su file system");

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

        JavaRDD<String> toJson = record
                .map( tuple -> {
                    Tuple2<Integer, Tuple2<String, Iterable<Tuple2<Integer, Iterable<Tuple2<Integer, Tuple4<Double, Double, Double, Double>>>>>>> tp = new Tuple2<>(fileType,tuple);
                    return new Gson().toJson(tp);
                });


        //String json = new Gson.tojson(classe);
        toJson.saveAsTextFile(Constants.HDFS_MONGO_QUERY2 +fileType);

    }
}

