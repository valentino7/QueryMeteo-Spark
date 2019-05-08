package sparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SQLQuery1 {

    private static String inputPath = "data/city_attributes.csv";
    private static String inputPath2 = "data/weather_description.csv";

    public static void executeQuery(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query1").master("local")
                 //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset df = spark.read().format("csv").option("header", "true").load(inputPath2);
        df.show();

        spark.stop();
    }

}
