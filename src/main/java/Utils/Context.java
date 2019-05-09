package Utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Context {

    public static JavaSparkContext getContext(String name){

        // SparkContext creation
        SparkConf conf = new SparkConf()
                .setMaster(Constants.MASTER)
                .setAppName(name);
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

}
