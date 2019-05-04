package first_query;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;


import java.time.LocalDate;
import java.util.HashMap;
import java.util.Iterator;
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

        JavaPairRDD<LocalDate, Record> pairs =
                weatherRDD.mapToPair(s -> new Tuple2<>(s.getDate(), s));



        //filtro per mese
        //JavaRDD<Record> d = weatherRDD.filter(m -> m.getDate().getMonthValue()> 2 && m.getDate().getMonthValue()<6);

        for(Tuple2 r: pairs.collect()) {
            System.out.println(r._2 + "\n");

        }

        //data.saveAsObjectFile(outputPath);

        sc.stop();
    }
}
