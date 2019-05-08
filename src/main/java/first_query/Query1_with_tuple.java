package first_query;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class Query1_with_tuple {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static String inputPath = "data/city_attributes.csv";
    private static String inputPath2 = "data/weather_description.csv";

    public static void main(String[] args) {

        String outputPath = "output";
        if (args.length > 0)
            outputPath = args[0];

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> weatherFile = sc.textFile(inputPath2);
        String firstLine = weatherFile.first();
        List<String> citiesArray = new ArrayList<>(Arrays.asList(firstLine.split(",")));
        citiesArray.remove(0);

        //creo rdd di città
        JavaRDD<String> cities = sc.parallelize(citiesArray);

        //formato (nome città, indice)
        JavaPairRDD<String, Integer> citiesWithIndex = cities.zipWithIndex().
                mapToPair(x -> new Tuple2<String, Integer>(x._1, x._2.intValue()));


        //change index, la chiave diventa l'indice della città e il valore il nome della città
        JavaPairRDD<Integer, String> citiesWithInvertedIndex = citiesWithIndex.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });

        for (Tuple2<Integer, String> x : citiesWithInvertedIndex.collect()) {
            System.out.println(x._1 + "   " + x._2);
        }

      /*  JavaRDD<String[]> csvRecords = weatherFile
                .filter( csvLine -> !csvLine.equals(fisrtLine) )
                .map(s -> s.split(",", -1) );

        JavaRDD<WeatherDescription> weather = csvRecords.flatMap((FlatMapFunction<String[], WeatherDescription>) strings -> {

            List<WeatherDescription> WDList = new ArrayList<>();

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = df.parse(strings[0]);
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTime(date);
            for (int i = 1; i < strings.length; i++) {
                WeatherDescription weatherDescription = new WeatherDescription();
                weatherDescription.setDescription(strings[i]);
                weatherDescription.setDate(cal);
                weatherDescription.setCity(i);
                WDList.add(weatherDescription);
            }

            return WDList.iterator();
        });


        // mapping da WeatherDescription a (Anno,Mese,Giorno,Città),( descrizione (1 o 0 in base se la stringa è quella che ci interessa)
        JavaPairRDD<Tuple4<Integer,Integer,Integer,Integer>,Integer> tuple = weather.mapToPair(new PairFunction<WeatherDescription, Tuple4<Integer, Integer, Integer,Integer>, Integer>() {
            @Override
            public Tuple2<Tuple4<Integer, Integer, Integer,Integer>, Integer> call(WeatherDescription weatherDescription) throws Exception {
                if (weatherDescription.getDescription().equals("sky is clear") ){
                    return new Tuple2<>( new Tuple4<>(weatherDescription.getDate().get(Calendar.YEAR),weatherDescription.getDate().get(Calendar.MONTH) ,weatherDescription.getDate().get(Calendar.DAY_OF_MONTH), weatherDescription.getCity() ), 1);
                }else {
                    return new Tuple2<>(new Tuple4<>(weatherDescription.getDate().get(Calendar.YEAR),weatherDescription.getDate().get(Calendar.MONTH) , weatherDescription.getDate().get(Calendar.DAY_OF_MONTH), weatherDescription.getCity() ), 0);
                }
            }
        });
*/

      /*
      parsing del file in rdd nella forma (anno, mese, giorno, indice città; sky is clear)
       */
        JavaPairRDD<Tuple4<Integer,Integer,Integer,Integer>,Integer> tupleP = weatherFile
                .filter( csvLine -> !csvLine.equals(firstLine) )
                .flatMapToPair(new PairFlatMapFunction<String, Tuple4<Integer, Integer, Integer, Integer>, Integer>() {
                    @Override
                    public Iterator<Tuple2<Tuple4<Integer, Integer, Integer, Integer>, Integer>> call(String s) throws Exception {
                        String[] strings = s.split(",", -1);
                        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = df.parse(strings[0]);
                        GregorianCalendar cal = new GregorianCalendar();
                        cal.setTime(date);
                        List<Tuple2<Tuple4<Integer,Integer,Integer,Integer>,Integer>> list = new ArrayList<>();
                        for (int i = 1; i < strings.length; i++) {
                            list.add( new Tuple2<>(new Tuple4<>(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH),i ), strings[i].equals("sky is clear") ? 1 : 0));
                        }
                        return list.iterator();

                    }
                });

        // filtro per prendere solo i mesi marzo,aprile, maggio
        JavaPairRDD<Tuple4<Integer,Integer,Integer,Integer>,Integer> filteredTuple = tupleP.filter( object -> (
                object._1._2() == 2 ||
                        object._1._2() == 3 ||
                        object._1._2() == 4 ) );

        //count delle ore in cui il tempo è sereno
        JavaPairRDD<Tuple4<Integer,Integer,Integer,Integer>,Integer> reducedTuple = filteredTuple.reduceByKey( (integer, integer2) -> integer+integer2) ;


        // io ho ( anno, mese, giorno, città ; 0/1) -> 1 se sky il clear, 0 else
        JavaPairRDD<Tuple3<Integer,Integer,Integer>, Integer> mapByMonth = reducedTuple.mapToPair(new PairFunction<Tuple2<Tuple4<Integer, Integer, Integer, Integer>, Integer>, Tuple3<Integer, Integer, Integer>, Integer>() {
            @Override
            public Tuple2<Tuple3<Integer, Integer, Integer>, Integer> call(Tuple2<Tuple4<Integer, Integer, Integer, Integer>, Integer> tuple) throws Exception {
                if (tuple._2 > 12){ //se almeno 12 ore in un giorno sono serene
                    return new Tuple2<>(new Tuple3<>(tuple._1._1(),tuple._1._2(),tuple._1._4()), 1);
                }else {
                    return new Tuple2<>(new Tuple3<>(tuple._1._1(),tuple._1._2(),tuple._1._4()), 0);
                }
            }
        });


        // sommo sui mesi, contando i giorni sereni in un mese
        JavaPairRDD<Tuple3<Integer,Integer,Integer>, Integer> reducedByMonth = mapByMonth.reduceByKey((integer, integer2) -> integer+integer2);

        //TODO: possono essere fatte insieme queste operazioni di conteggio e filtering?
        JavaPairRDD<Tuple3<Integer,Integer,Integer>, Integer> resultFiltered = reducedByMonth.filter(new Function<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple3<Integer, Integer, Integer>, Integer> tuple) throws Exception {
                if (tuple._2 >= 15) {// città con più di 15 giorni al mese con tempo sereno
                    return true;
                }
                else return false;
            }
        });


        //compatto la data in una stringa per avere (data, indice di città)
        JavaPairRDD<String, Integer> result = resultFiltered.mapToPair(new PairFunction<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Tuple3<Integer, Integer, Integer>, Integer> tuple) throws Exception {
                String yearStr = tuple._1._1().toString();
                String monthStr = tuple._1._2().toString();
                String date = yearStr.concat("/").concat(monthStr);
                return new Tuple2<>(date, tuple._1._3());
            }
        });

        //Change index, la chiave diventa l'indice della città e il valore la data
        JavaPairRDD<Integer, String> invertedIndexResult = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }

        });


        for (Tuple2<Integer, String> x: invertedIndexResult.collect()) {
            System.out.println(x._1 + "   " + x._2);
        }


        /*JavaPairRDD<Tuple2<Integer,Integer>, Integer> result = resultFiltered.mapToPair(new PairFunction<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>, Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Integer> call(Tuple2<Tuple3<Integer, Integer, Integer>, Integer> tuple) throws Exception {
                return new Tuple2<>(new Tuple2<>(tuple._1._1(),tuple._1._2()), tuple._1._3());
            }
        });*/




        // mappo da ( anno , mese , città ) in ( anno -> città ) con la condizione di cercare i mesi con almeno 15  giorni di cielo sereno
     /*   JavaPairRDD<Tuple2<Integer,Integer>, Integer> result = reducedByMonth.mapToPair(new PairFunction<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>, Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Integer> call(Tuple2<Tuple3<Integer, Integer, Integer>, Integer> tuple) throws Exception {
                if (tuple._2 >= 15) {// città con più di 15 giorni al mese con tempo sereno
                    return new Tuple2<>(new Tuple2<>(tuple._1._1(), tuple._1._2() ),tuple._1._3());
                }
                return null;
            }
        });*/



         //join dei due rdd con chiave in comune
         JavaPairRDD<Integer, Tuple2<String, String>> joinResult = invertedIndexResult.join(citiesWithInvertedIndex);

         for(Tuple2<Integer, Tuple2<String, String>> x :joinResult.collect()) {
             System.out.println(x._1 + "  " + x._2._1() +"   " + x._2._2());
         }

        /*
           esclusione dell'indice di città, la chiave è la data e il valore è la città
           raggruppo per chiave
           sorting per chiave
         */
         JavaPairRDD<String, Iterable<String>> citiesWithClearSky = joinResult.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<String, String>>, String, String>() {
             @Override
             public Tuple2<String, String> call(Tuple2<Integer, Tuple2<String, String>> integerTuple2Tuple2) throws Exception {
                 return new Tuple2<>(integerTuple2Tuple2._2._1, integerTuple2Tuple2._2._2);
             }
         }).groupByKey().sortByKey();


        for(Tuple2<String, Iterable<String>> x :citiesWithClearSky.collect()) {
            System.out.println(x._1 + "  " + x._2);
        }


        /*// anno -> lista di città in interi
        Map<Tuple2<Integer,Integer>,Iterable<Integer>> map = result.groupByKey().collectAsMap();
        for ( Tuple2<Integer,Integer> i : map.keySet()){
            System.out.println("Anno: "+ i._1() +"\t" +"Mese: "+ i._2() +"\t" +"Indici Città: "+"\t" +map.get(i));
        }*/

/*
        List<WeatherDescription> list = weather.collect();
        for ( WeatherDescription w : list)
            System.out.println(w.getDate().get(Calendar.HOUR_OF_DAY));
*/
      /*  Map<Tuple3<Integer, Integer, Integer>, Iterable<Integer>> map = result.groupByKey().collectAsMap();
        for ( Tuple3<Integer, Integer, Integer> m : map.keySet()) {
            System.out.println("Anno: " + m._1() + "\t Mese: " + m._2() + "\t Città: " + m._3() + "\t" +" Count: "+ map.get(m) );

        }*/



/*
        JavaRDD<String[]> csvRecords = sc.textFile(inputPath2)
                .map(s -> s.split(",", -1) );

        String[] citiesTemp = csvRecords.first();
        JavaRDD<String[]> filteredCsvRecords = csvRecords.filter(x -> !Arrays.equals(x, citiesTemp));

        JavaPairRDD<String,List<Integer>> p = filteredCsvRecords.mapToPair(new PairFunction<String[], String, List<Integer>>() {
            @Override
            public Tuple2<String, List<Integer>> call(String[] strings) throws Exception {

                String date = null;
                List<Integer> list = new ArrayList<>();

                for (int i = 0; i < strings.length; i++) {
                    if (i == 0) {/*
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);
                        date = LocalDate.parse(strings[i], formatter);

                        String[] s = strings[i].split(" ");
                        date = s[0];

                    } else {
                        if (strings[i].equals("sky is clear") )
                            // si possono aggiungere altre stringhe per capire se il tempo è sereno
                            list.add(1);
                        else
                            list.add(0);
                    }
                }
                return new Tuple2<>(date,list);
            }
        });


        JavaPairRDD<String,List<Integer>> aggregateRDD = p.reduceByKey((Function2<List<Integer>, List<Integer>, List<Integer>>) (integers, integers2) -> {
            List<Integer> temp = new ArrayList<>();

            for (int i = 0 ; i < integers.size(); i++){
                temp.add(integers.get(i)+integers2.get(i));
            }

            return temp;
        });

        // mappo in un RDD del tipo
        // <Data, indici dell'array in cui un certo valore è > 15 ( ovvero più di 15 ore al giorno sono serene)>

        Map<String,List<Integer>> map = aggregateRDD.collectAsMap();

        for ( String d : map.keySet()){
            System.out.println(d + " -> " + map.get(d) );
        }


        //System.out.println(cityRDD.collect());

        JavaRDD<Record> weatherRDD = Query1Preprocessing.preprocessDataset(sc);

        JavaPairRDD<LocalDate, HashMap<String,String>> pairs =
                weatherRDD.mapToPair(s -> new Tuple2<>(s.getDate(), s.getWeather_city()));


        //filtro per mese
        //JavaRDD<Record> d = weatherRDD.filter(m -> m.getDate().getMonthValue()> 2 && m.getDate().getMonthValue()<6);


       for(Tuple2 r: pairs.collect()) {
            System.out.println(r._2 + "\n");

        }

        //data.saveAsObjectFile(outputPath);
*/
        sc.stop();
    }
}
