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
import scala.Tuple4;
import sparkCore.Query1;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static Controllers.ControllerQuery1.convertToDataset;

public class TestQuery1 {

    public static void main(String[] args) {



        BufferedWriter writer = null;
        CSVPrinter csvPrinter = null;

        try {
            writer = Files.newBufferedWriter(Paths.get("test/query1.csv"));;

            csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("FileLoadingTime", "RestApiTime","PreProcessTime", "ExecutionTime", "WriteTime"));

            Map<String, Tuple2<String, String>> country = null;
            Dataset<Row> city_data = null;

            for (int j = 0; j < 1; j++) {

                SparkSession spark = Context.getContext("test query 1 sparkCore core");


                Stopwatch watchLoadFile = Stopwatch.createStarted();
                Dataset<Row> inputData = null;
                switch (j) {
                    case 0:
                        inputData = spark.read().option("header", "true").csv("input/" + Constants.WEATHER_FILE_CSV);
                        city_data = spark.read().option("header","true").csv("input/" + Constants.CITY_FILE_CSV);
                        break;

                    case 1:
                        inputData = spark.read().option("header", "true").parquet(Constants.HDFS_INPUT  + Constants.WEATHER_FILE_PARQUET);
                        city_data = spark.read().option("header","true").parquet(Constants.HDFS_INPUT + Constants.CITY_FILE_PARQUET);
                        break;
                }

                String tmpLoadTime = watchLoadFile.stop().toString();
                tmpLoadTime = tmpLoadTime.replace(",", ".");
                tmpLoadTime = tmpLoadTime.replace(",", ".");


                Stopwatch watchRest = Stopwatch.createStarted();
                country = Nations.getNation(spark ,city_data);
                String tmpRestTime = watchRest.stop().toString();
                tmpRestTime = tmpRestTime.replace(",", ".");

                for (int i = 0; i < 50; i++) {


                    Stopwatch watchPre = Stopwatch.createStarted();
                    JavaRDD<Tuple3<String, String, Double>> values = AllQueryPreProcess.executePreProcess(inputData, 1);
                    JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> data = Query1Preprocess.executeProcess(country, values);
                    String tmpTime = watchPre.stop().toString();
                    tmpTime = tmpTime.replace(",", ".");


                    Stopwatch watchExe = Stopwatch.createStarted();
                    JavaPairRDD<Integer, String> result = Query1.executeQuery(data);
                    String tmpTime2 = watchExe.stop().toString();
                    tmpTime2 = tmpTime2.replace(",", ".");


                    Stopwatch watchWrite = Stopwatch.createStarted();
                    Dataset<Row> resultsDS= convertToDataset(spark,result);
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
