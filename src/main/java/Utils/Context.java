package Utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Context {

    public static SparkSession getContext(String name){

        // SparkContext creation
        //startTimer
        SparkSession spark = SparkSession
                .builder()
                .appName(name)
                //.master(Constants.MASTER)
                //.config("sparkCore.some.config.option", "some-value")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        return spark;
    }

}
