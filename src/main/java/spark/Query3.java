package spark;

import Utils.Constants;
import Utils.Context;
import Utils.PreProcess;
import Utils.TupleComparator;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import scala.*;

import java.lang.Boolean;
import java.lang.Double;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Query3 {

    public static void executeQuery(String[] args){


        JavaSparkContext sc= Context.getContext("Query3");

        JavaRDD<String> attributes_file = sc.textFile(Constants.TEMPERATURE_FILE);
        String firstLine = attributes_file.first();
        List<String> citiesArray = new ArrayList<>(Arrays.asList(firstLine.split(",")));
        citiesArray.remove(0);

        // hashmap <Città,(Nazione,TimeZoneID)>
        Map<String, Tuple2<String,String>> city_nations = PreProcess.executeProcess(sc);

        // convertire tutte le date nell'orario locale
        JavaPairRDD<Tuple2<Integer,String>, Iterable<Tuple2<String, Double >>> result = attributes_file
                .filter( csvLine -> !csvLine.equals(firstLine) )
                .flatMapToPair((PairFlatMapFunction<String, Tuple5<Integer,Integer,Integer, String, String>, Tuple2<Double,Double> >) s -> {
                    String[] strings = s.split(",", -1);

                    double d = 0.0;
                    List< Tuple2< Tuple5<Integer,Integer,Integer, String, String>, Tuple2<Double,Double>> > list = new ArrayList<>();
                    for (int i = 1; i < strings.length; i++) {
                        if(strings[i].isEmpty())
                            d=0.0;
                        else
                            d= Double.parseDouble(strings[i]);
                        if (d>= 350)
                            d=d/1000;
                        if (d>=1000000)
                            d=d/10000;

                        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = df.parse(strings[0]);

                        DateTimeZone timeZone = DateTimeZone.forID( city_nations.get(citiesArray.get(i-1))._2);

                        DateTime dateTime = new DateTime( date, timeZone );

                        list.add( new Tuple2<>(new Tuple5<>(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getHourOfDay(), city_nations.get(citiesArray.get(i-1))._1(),citiesArray.get(i-1) ) , new Tuple2<>(d,1.0) ) );
                    }
                    return list.iterator();
                })
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

        Map<Tuple2<Integer,String>, Iterable<Tuple2<String,Double>> > map = result.collectAsMap();
        for (Tuple2<Integer,String> t : map.keySet()){
            System.out.println(t +"\t" + map.get(t));
        }
        //result.saveAsTextFile("result");
        sc.stop();
    }
}
