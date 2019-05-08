package sparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SQLQuery2 {

    public static void executeQuery(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query1").master("local")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        spark.stop();
    }
}
