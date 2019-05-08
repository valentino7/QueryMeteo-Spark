package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Query2 {

    public static void executeQuery(String[] args){

        // SparkContext creation
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.stop();
    }
}
