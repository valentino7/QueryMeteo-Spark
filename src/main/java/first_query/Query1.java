package first_query;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;


import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

public class Query1 {

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

        JavaRDD<String> cityRDD = sc.textFile(inputPath)
                .map(s -> s.split(",")[0]);



        //System.out.println(cityRDD.collect());

        JavaRDD<Record> weatherRDD = Query1Preprocessing.preprocessDataset(sc);

        /*JavaPairRDD<LocalDate, HashMap<String,String>> pairs =
                weatherRDD.mapToPair(s -> new Tuple2<>(s.getDate(), s.getWeather_city()));*/



        JavaPairRDD<LocalDate, ArrayList<String>> pairs = weatherRDD.flatMapToPair(new CityExtractor());




        //filtro per mese
        //JavaRDD<Record> d = weatherRDD.filter(m -> m.getDate().getMonthValue()> 2 && m.getDate().getMonthValue()<6);

        for(Tuple2 r: pairs.collect()) {
            System.out.println(r + "\n");

        }

        //data.saveAsObjectFile(outputPath);

        sc.stop();
    }

    private static class CityExtractor implements PairFlatMapFunction<Record, LocalDate, ArrayList<String>> {

        @Override
        public Iterator<Tuple2<LocalDate,ArrayList<String>>> call(Record record) throws Exception {

            ArrayList<Tuple2<LocalDate,ArrayList<String>>> results = new ArrayList<>();
            //Iterator it = record.getWeather_city().entrySet().iterator();


            for (Map.Entry<String, String> entry : record.getWeather_city().entrySet()) {
                ArrayList<String> e= new ArrayList<>();
                e.add(entry.getKey());
                e.add(entry.getValue());


                Tuple2<LocalDate,ArrayList<String>> result = new Tuple2<>(record.getDate(),e );
                results.add(result);


            }

            return results.iterator();

        }
    }
}
