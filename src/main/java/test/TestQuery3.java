package test;

import Utils.*;
import com.google.common.base.Stopwatch;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import sparkCore.Query3;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static Controllers.ControllerQuery3.convertToDataset;

public class TestQuery3 {

    public static void main(String[] args) {

        ;
        BufferedWriter writer = null;
        CSVPrinter csvPrinter = null;

        try {

            SparkSession spark = Context.getContext("test query 3 sparkCore core");

            writer = Files.newBufferedWriter(Paths.get("test/query3.csv"));

            csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("FileLoadingTime", "RestApiTime", "PreProcessTime", "ExecutionTime", "WriteTime"));

            for (int j = 0; j < 1; j++) {

                Stopwatch watchLoadFile = Stopwatch.createStarted();
                Dataset<Row> inputData = null;
                Map<String, Tuple2<String, String>> country = null;
                Dataset<Row> city_data = null;
                switch (j) {
                    case 0:
                        inputData = spark.read().option("header", "true").csv("input/" + Constants.TEMPERATURE_FILE_CSV);
                        city_data = spark.read().option("header", "true").csv("input/" + Constants.CITY_FILE_CSV);
                        break;

                    case 1:
                        inputData = spark.read().option("header", "true").parquet(Constants.HDFS_INPUT + Constants.TEMPERATURE_FILE_PARQUET);
                        city_data = spark.read().option("header", "true").parquet(Constants.HDFS_INPUT + Constants.CITY_FILE_PARQUET);
                        break;
                }

                String tmpLoadTime = watchLoadFile.stop().toString();
                tmpLoadTime = tmpLoadTime.replace(",", ".");
                tmpLoadTime = tmpLoadTime.replace(",", ".");


                Stopwatch watchRest = Stopwatch.createStarted();
                country = Nations.getNation(spark, city_data);
                String tmpRestTime = watchRest.stop().toString();
                tmpRestTime = tmpRestTime.replace(",", ".");


                for (int i = 0; i < 50; i++) {


                    Stopwatch watchPre = Stopwatch.createStarted();
                    JavaRDD<Tuple3<String, String, Double>> values = AllQueryPreProcess.executePreProcess(inputData, 1).cache();
                    JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> data = Query3Preprocess.executeProcess(country,values);
                    String tmpTime = watchPre.stop().toString();
                    tmpTime = tmpTime.replace(",", ".");


                    Stopwatch watchExe = Stopwatch.createStarted();
                    JavaPairRDD<String, List<Tuple2<String,Integer> >>  result = Query3.executeQuery(data);
                    String tmpTime2 = watchExe.stop().toString();
                    tmpTime2 = tmpTime2.replace(",", ".");


                    Stopwatch watchWrite = Stopwatch.createStarted();
                    Dataset<Row> resultsDS = convertToDataset(spark, result);
                    //resultsDS.write().format("parquet").option("header", "true").save(Constants.HDFS_HBASE_QUERY1);
                    //resultsDS.write().format("csv").option("header", "true").save(Constants.HDFS_HBASE_QUERY1);
                    resultsDS.coalesce(1).write().format("json").option("header", "true").save("results/file" + i);
                    String tmpWrite = watchWrite.stop().toString();
                    tmpWrite = tmpWrite.replace(",", ".");


                    csvPrinter.printRecord(tmpLoadTime, tmpRestTime, tmpTime, tmpTime2, tmpWrite);
                    csvPrinter.flush();
                }

                spark.stop();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                csvPrinter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }



}
