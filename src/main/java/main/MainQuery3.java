package main;

import Utils.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

import sparkSQL.SQLQuery3;
import spark_v2.Query3_v2;

import java.util.Map;

public class MainQuery3 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query1").master("local")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        //Nations
        Map<String, Tuple2<String,String>> nations = Nations.getNation(sc);

        JavaRDD<Tuple3<String,String,Double>> valuesq3 = AllQueryPreProcess.executePreProcess(sc, Constants.TEMPERATURE_FILE,3);

        JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> preprocess = Query3Preprocess.executeProcess(nations,valuesq3);

        /*  Query3_v2.executeQuery(preprocess);


*/
        SQLQuery3.executeQuery(spark,preprocess);


        spark.stop();
    }
}
