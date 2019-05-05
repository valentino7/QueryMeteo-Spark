package first_query;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static java.lang.Thread.sleep;


public class Query1 {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static String inputPath = "data/city_attributes.csv";
    private static String inputPath2 = "data/weather_description.csv";

    public static void main(String[] args) throws InterruptedException {

        String outputPath = "output";
        if (args.length > 0)
            outputPath = args[0];

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> cityRDD = sc.textFile(inputPath)
                .map(s -> s.split(",")[0]);



        JavaRDD<Record> weatherRDD = Query1Preprocessing.preprocessDataset(sc);


        //creo una chiave data->arraylist(citta,1 se è sereno e 0 altrimenti)
        JavaPairRDD<LocalDate, HashMap<String,Integer>> pairs = weatherRDD.mapToPair(new CityExtractor());



        Function2<HashMap<String,Integer>,HashMap<String,Integer> ,HashMap<String,Integer > >f2 = (y,x) -> {
            // do stuff
            y.forEach((k, v) -> x.merge(k, v, Integer::sum));
            return x;

        };
        //sommo gli 1 delle ore serene ottendendo data-> (citta,totali ore serene giornaliere) e poi filtro per mese
        //reduce aggrega date uguali eliminando le ore
        JavaPairRDD<LocalDate, HashMap<String,Integer > >day = pairs.reduceByKey(f2).filter(m ->m._1.getMonthValue() > 2 && m._1.getMonthValue()<6);




        //metto 1 se il valore sommato precedentemente è maggiore di 12 ore serene ottenendo i giorni sereni
        //creo una chiave formata da mese-anno
        JavaPairRDD<String, HashMap<String,Integer>> p = day.mapToPair(
                    new PairFunction<Tuple2<LocalDate, HashMap<String,Integer>>, String, HashMap<String,Integer>>() {
            @Override
            public Tuple2<String, HashMap<String,Integer>> call(Tuple2<LocalDate, HashMap<String,Integer>> localDateArrayListTuple2) throws Exception {

                HashMap<String,Integer> e= new HashMap<>();
                Tuple2<String,HashMap<String,Integer>> result;

                for (Map.Entry<String, Integer> entry : localDateArrayListTuple2._2.entrySet()) {

                    if(entry.getValue()>12)
                        e.putIfAbsent(entry.getKey(),1);
                    else
                        e.putIfAbsent(entry.getKey(),0);

                }
                result = new Tuple2<>(localDateArrayListTuple2._1.getMonthValue()+"-"+localDateArrayListTuple2._1.getYear(),e );

                return result;
            }
        });




        //sommo gli 1 dei giorni sereni ottenendo il numero di giorni in cui il tempo è sereno
        //reduce aggrega per mese-anno eliminando i giorni
        JavaPairRDD<String,HashMap<String,Integer> > c = p.reduceByKey( (x,y) -> {
                    // do stuff
                    y.forEach((k, v) -> x.merge(k, v, Integer::sum));


                    return x;
                }
        );



        //metto 1 se il contatore dei giorni sereni in un mese è maggiore di 15
        JavaPairRDD<String, HashMap<String,Integer>> f = c.mapToPair(
                new PairFunction<Tuple2<String, HashMap<String,Integer>>, String, HashMap<String,Integer>>() {
                    @Override
                    public Tuple2<String, HashMap<String,Integer>> call(Tuple2<String, HashMap<String,Integer>> localDateArrayListTuple2) throws Exception {

                        HashMap<String,Integer> e= new HashMap<>();
                        Tuple2<String,HashMap<String,Integer>> result;

                        for (Map.Entry<String, Integer> entry : localDateArrayListTuple2._2.entrySet()) {

                            if(entry.getValue()>14)
                                e.putIfAbsent(entry.getKey(),entry.getValue());

                        }
                        result = new Tuple2<>(localDateArrayListTuple2._1,e );

                        return result;
                    }
                });



        Tuple2 t=pairs.collect().get(0);
        int i=0;
        for(Tuple2 r: f.collect()) {
            //System.out.println(t._1 + "\n");
            System.out.println(r + "\n");
        }

        //data.saveAsObjectFile(outputPath);

        sc.stop();
    }

    private static class CityExtractor implements PairFunction<Record, LocalDate, HashMap<String,Integer>> {

        @Override
        public Tuple2<LocalDate, HashMap<String,Integer>> call(Record record) throws Exception {

            //Iterator it = record.getWeather_city().entrySet().iterator();
            HashMap<String,Integer> e= new HashMap<>();
            Tuple2<LocalDate,HashMap<String,Integer>> result;

            for (Map.Entry<String, String> entry : record.getWeather_city().entrySet()) {
                //e.add(entry.getKey());
                //e.add(entry.getValue());
                if(entry.getValue().equals("sky is clear"))
                    e.putIfAbsent(entry.getKey(),1);
                else
                    e.putIfAbsent(entry.getKey(),0);

            }
            result = new Tuple2<>(record.getDate(),e );

            return result;

        }
    }





}
