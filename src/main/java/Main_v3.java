import Utils.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.*;
import scala.collection.TraversableOnce;
import sparkSQL.SQLQuery1;
import sparkSQL.SQLQuery2;
import sparkSQL.SQLQuery3;
import spark_v2.Query1_v2;
import spark_v2.Query2_v2;
import spark_v2.Query3_v2;

import java.lang.Double;
import java.util.Map;


public class Main_v3 {

    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("Spark")
                .master("local")
                .config("spark.mongodb.output.uri", "mongodb://172.18.0.2/queryDB.query1")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        //Nations
        Map<String, Tuple2<String,String>> nations = Nations.getNation(sc);
/*
       //Query1
        JavaPairRDD<String,String>prova  = spark.read()
                .parquet()
                .toJavaRDD()
                .mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return null;
            }
        });


        //Preprocess
        JavaRDD<Tuple3<String,String,Double>>values = AllQueryPreProcess.executePreProcess(sc,Constants.WEATHER_FILE,1);
        JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> data = Query1Preprocess.executeProcess(nations,values);

        //START time
        Query1_v2.executeQuery(data);
        //STOP time



        //Query2

        for (int i = 2 ; i < Constants.STATISTICS_FILE; i++) {
            //Start Preprocess
            JavaRDD<Tuple3<String, String, Double>> valuesq2 = AllQueryPreProcess.executePreProcess(sc, Constants.FILE[i], 2);
            // (Year,Month,Nation) , (Value, count)
            JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>> dt = Query2Preprocess.executeProcess(nations, valuesq2, i);

            //START time
            Query2_v2.executeQuery(dt, i);
            //STOP time
        }
*/
        //Query3

        JavaRDD<Tuple3<String,String,Double>> valuesq3 = AllQueryPreProcess.executePreProcess(sc,Constants.TEMPERATURE_FILE,3);
        JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> preprocess = Query3Preprocess.executeProcess(nations,valuesq3);
      //START time
 /*       Query3_v2.executeQuery(preprocess);
        //STOP time

        //SQL



       //Query_SQL1

        //START time
        SQLQuery1.executeQuery(spark,data);
        //STOP time

        //Query_SQL2

        //START time
        SQLQuery2.executeQuery(args);
        //STOP time

*/
        //Query_SQL3

        //START time
        SQLQuery3.executeQuery(spark,preprocess);
        //STOP time

    }
}
