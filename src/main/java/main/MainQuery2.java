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
import sparkSQL.SQLQuery2;
import spark_v2.Query2_v2;

import java.util.Map;

public class MainQuery2 {

    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query2").master("local")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        sc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        //Nations
        Dataset<Row> city_file = spark.read().option("header","true").csv("input/" +Constants.CITY_FILE_CSV);

        //Nations
        Map<String, Tuple2<String,String>> country = Nations.getNation(spark, city_file);

        for (int i = 0; i < Constants.STATISTICS_FILE; i++) {

            Dataset<Row> inputData = spark.read().parquet(Constants.HDFS_INPUT +Constants.FILE[i]);

            JavaRDD<Tuple3<String, String, Double>> valuesq2 = AllQueryPreProcess.executePreProcess(inputData,  2);

            // (Year,Month,Nation) , (Value, count)
            JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>> dt = Query2Preprocess.executeProcess(country, valuesq2, i);

           // Query2_v2.executeQuery(dt, i);

            SQLQuery2.executeQuery(spark,dt);
        }

        spark.stop();

    }
}
