package spark;

import Utils.Constants;
import Utils.Context;
import Utils.PreProcess;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple6;

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
        JavaPairRDD<Tuple6<Integer,Integer,Integer,Integer,String,String>, Double > dataset = attributes_file
                .filter( csvLine -> !csvLine.equals(firstLine) )
                .flatMapToPair((PairFlatMapFunction<String, Tuple6<Integer,Integer,Integer, Integer, String, String>, Double >) s -> {
                    String[] strings = s.split(",", -1);

                    double d = 0.0;
                    List<Tuple2<Tuple6<Integer,Integer,Integer,Integer,String,String>, Double > > list = new ArrayList<>();
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
                        //DateTime dateTimeUtc = dateTime.withZone( DateTimeZone.UTC );


                        if ( citiesArray.get(i-1).equals("Portland")){
                            System.out.println(dateTime.toString()+ "|\t|" + strings[0]);
                            System.out.println("------------------------------------");
                        }

                        list.add( new Tuple2<>(new Tuple6<>(dateTime.getYear(), dateTime.getMonthOfYear(),dateTime.getDayOfMonth(), dateTime.getHourOfDay(), city_nations.get(citiesArray.get(i-1))._1(),citiesArray.get(i-1) ), d));
                    }
                    return list.iterator();
                });

        dataset.saveAsTextFile("prova");
        sc.stop();
    }
}
