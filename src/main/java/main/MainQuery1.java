package main;

import Utils.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import sparkSQL.SQLQuery1;
import spark_v2.Query1_v2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MainQuery1 {


    public static void executeMain() {

        //startTimer
        SparkSession spark = Context.getContext("query1");

        /*
        inputData = spark.read().parquet(Constants.HDFS +Constants.WEATHER_FILE);
        inputData = spark.read().csv(Constants.HDFS +Constants.WEATHER_FILE);
        */

        Dataset<Row> inputData = spark.read().option("header","true").csv("input/" +Constants.WEATHER_FILE_CSV);
        Dataset<Row> city_file = spark.read().option("header","true").csv("input/" +Constants.CITY_FILE_CSV);

        //Nations
        Map<String, Tuple2<String,String>> country = Nations.getNation(spark, city_file);

        JavaRDD<Tuple3<String,String,Double>> values = AllQueryPreProcess.executePreProcess(inputData,1).cache();

        JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> data = Query1Preprocess.executeProcess(country,values).cache();


//stop time
       //startTime
        Query1_v2.executeQuery(data);

//stop time

        //startTime

        SQLQuery1.executeQuery(spark,data);

//stop time

        spark.stop();

    }

    public static void main(String[] args) {

        MainQuery1.executeMain();
    }

}
