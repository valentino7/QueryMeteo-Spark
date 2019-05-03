package first_query;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.regex.Pattern;

public class Query1 {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static String inputPath = "data/weather_description.csv";

    public static void main(String[] args) {



        String outputPath = "output";
        if (args.length > 0)
            outputPath = args[0];

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile(inputPath);
        JavaRDD<Record> value = Query1Preprocessing.preprocessDataset(sc);

        //value.saveAsObjectFile(outputPath);

        sc.stop();
    }
}
