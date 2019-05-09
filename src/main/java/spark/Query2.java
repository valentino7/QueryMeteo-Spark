package spark;

import Utils.Context;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Query2 {

    public static void executeQuery(String[] args){

        JavaSparkContext sc=Context.getContext("Query2");





        sc.stop();
    }
}
