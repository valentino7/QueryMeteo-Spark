package test;

import Utils.*;
import com.google.common.base.Stopwatch;
import main.MainQuery1;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import spark_v2.Query1_v2;
import sun.applet.Main;

import java.io.*;
import java.net.ContentHandler;
import java.util.Map;

public class TestQuery1 {

    public static void main(String[] args) {

        SparkSession spark = Context.getContext("test query 1 spark core");

        PrintWriter writer = null;

        try {
            writer = new PrintWriter("test/file2.txt");


            for (int j = 0; j < 2; j++) {

                for (int i = 0; i < 10; i++) {
                    Stopwatch watchPre = Stopwatch.createStarted();
                    Dataset<Row> inputData = null;
                    Map<String, Tuple2<String, String>> country = null;
                    Dataset<Row> city_data = null;
                    switch (j) {
                        case 0:
                            inputData = spark.read().option("header", "true").csv(Constants.HDFS_INPUT + Constants.WEATHER_FILE_CSV);
                            city_data = spark.read().option("header","true").csv(Constants.HDFS_INPUT + Constants.CITY_FILE_CSV);
                            break;

                        case 1:
                            inputData = spark.read().option("header", "true").parquet(Constants.HDFS_INPUT  + Constants.WEATHER_FILE_PARQUET);
                            city_data = spark.read().option("header","true").parquet(Constants.HDFS_INPUT + Constants.CITY_FILE_PARQUET);
                            break;
                    }

                    country = Nations.getNation(spark ,city_data);
                    JavaRDD<Tuple3<String, String, Double>> values = AllQueryPreProcess.executePreProcess(inputData, 1).cache();
                    JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> data = Query1Preprocess.executeProcess(country, values).cache();
                    watchPre.stop();
                    writer.println("Preprocessing " + "\t"+j +watchPre.toString());

                    Stopwatch watchExe = Stopwatch.createStarted();
                    Query1_v2.executeQuery(data);
                    watchExe.stop();
                    writer.println("Execution "+ "\t"+j +watchExe.toString());

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writer.close();

        }
    }

}
