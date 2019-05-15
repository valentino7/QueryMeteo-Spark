package main;

import Utils.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import sparkSQL.SQLQuery1;
import spark_v2.Query1_v2;

import java.util.Map;

public class MainQuery1 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query1").master("local")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        sc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        //Nations
        Map<String, Tuple2<String,String>> nations = Nations.getNation(sc);

        JavaRDD<Tuple3<String,String,Double>> values = AllQueryPreProcess.executePreProcess(sc, Constants.WEATHER_FILE,1);
        JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> data = Query1Preprocess.executeProcess(nations,values);


        Query1_v2.executeQuery(data);


        //SQLQuery1.executeQuery(spark,data);


        spark.stop();
    }
}
