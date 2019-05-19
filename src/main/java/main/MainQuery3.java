package main;

import Utils.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

        sc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        Dataset<Row> inputData = spark.read().option("header","true").csv(Constants.HDFS_INPUT +Constants.TEMPERATURE_FILE_CSV);

        //Nations
        Dataset<Row> city_file = spark.read().option("header","true").csv("input/" +Constants.CITY_FILE_CSV);

        //Nations
        Map<String, Tuple2<String,String>> country = Nations.getNation(spark, city_file);

        JavaRDD<Tuple3<String,String,Double>> valuesq3 = AllQueryPreProcess.executePreProcess(inputData,3);

        JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> preprocess = Query3Preprocess.executeProcess(country,valuesq3);

        //getTIme
        Query3_v2.executeQuery(preprocess);
        //getTIme


        SQLQuery3.executeQuery(spark,preprocess);


        spark.stop();
    }
}
