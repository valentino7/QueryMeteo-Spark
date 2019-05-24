package sparkCore;


import Utils.Constants;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;


public class Query1 {


    public static JavaPairRDD<Integer, String> executeQuery(JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> values){

        /*

        .reduceByKey :
            Sum values with same day:
            In this way we calculate the number of "sky is clear" in a day

        .mapToPair :
             RDD<(year,month,day,city), count> to RDD<K,V> where:
                K : (year,month,city)
                V : 0 or 1
                    1 : count > N(18)
                        N is the number of "sky is clear" needed to consider "clear" a day
                    0 : else
        .reduceByKey :
            Sum values with same month:
            In this way we calculate the number of clear days in a month

        .filter :
            RDD<(year,month,city), count>  on count: take the Month with at least 15 clear day

        .mapToPair :
            RDD<(year,month,city), count>  to RDD< K,V> where :
                K = (year,city)
                V = 1.0

        .reduece: count the number of times the city appears
        .filter: take the city that appears in all three considered month
        .mapTopPair:
            RDD< year,cities), count> to RDD<K,V> where :
                K = year
                V= city
        .groupByKey : group by year
        .sortByKey : order by year
         */
        JavaPairRDD<Integer, String> citiesWithClearSky = values
                .reduceByKey(Double::sum)
                .mapToPair((PairFunction<Tuple2<Tuple4<Integer, Integer, Integer, String>, Double>, Tuple3<Integer, Integer, String>, Integer>) tuple -> {
                    if (tuple._2 >= Constants.CLEAR_DAY){
                        return new Tuple2<>(new Tuple3<>(tuple._1._1(),tuple._1._2(),tuple._1._4()), 1);
                    }else {
                        return new Tuple2<>(new Tuple3<>(tuple._1._1(),tuple._1._2(),tuple._1._4()), 0);
                    }
                })
                .reduceByKey(Integer::sum)
                .filter((Function<Tuple2<Tuple3<Integer, Integer, String>, Integer>, Boolean>) tuple -> tuple._2 >= Constants.CLEAR_MONTH)
                .mapToPair((PairFunction<Tuple2<Tuple3<Integer, Integer, String>, Integer>, Tuple2<Integer,String>,Integer>) tuple -> new Tuple2<>(new Tuple2<>(tuple._1._1(),tuple._1._3()),1))
                .reduceByKey(Integer::sum)
                .filter(v1 -> v1._2()>=3)
                .mapToPair(Tuple2::_1)
                .sortByKey()
                .cache();
       return citiesWithClearSky;

    }


}

