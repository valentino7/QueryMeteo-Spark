package main;

import Utils.*;
import com.twitter.chill.java.ArraysAsListSerializer;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.collection.immutable.Seq;
import sparkSQL.SQLQuery1;
import spark_v2.Query1_v2;

import javax.xml.crypto.Data;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

public class MainQuery1 {


    public static Dataset<Row> convertToDataset(SparkSession spark,JavaPairRDD<Integer, Iterable<String>> result){
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("year", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("cities", DataTypes.createArrayType(DataTypes.StringType), true));
        StructType schemata = DataTypes.createStructType(fields);


        JavaRDD<Row> rows = result.map(t -> {
            ArrayList<String> tam= new ArrayList<>();
            for (String te: t._2 ) {
                tam.add(te);
            }
            String[] item = tam.toArray(new String[tam.size()]);
            return RowFactory.create(t._1(), item  );
        });
        Dataset<Row> resultsDS = spark.sqlContext().createDataFrame(rows, schemata);
        return resultsDS;
    }

    public static void executeMain() {

        //startTimer
        SparkSession spark = Context.getContext("query1");

        /*
        inputData = spark.read().parquet(Constants.HDFS_INPUT +Constants.WEATHER_FILE);
        inputData = spark.read().csv(Constants.HDFS_INPUT +Constants.WEATHER_FILE);
        */

        Dataset<Row> inputData = spark.read().option("header","true").parquet(Constants.HDFS_INPUT +Constants.WEATHER_FILE_PARQUET);
        Dataset<Row> city_file = spark.read().option("header","true").parquet(Constants.HDFS_INPUT +Constants.CITY_FILE_PARQUET);

        //Nations
        Map<String, Tuple2<String,String>> country = Nations.getNation(spark, city_file);

        JavaRDD<Tuple3<String,String,Double>> values = AllQueryPreProcess.executePreProcess(inputData,1).cache();

        JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> data = Query1Preprocess.executeProcess(country,values).cache();

        JavaPairRDD<Integer, Iterable<String>> result = Query1_v2.executeQuery(data);

        Dataset<Row> resultsDS= convertToDataset(spark,result);
        resultsDS.show();
        resultsDS.write().format("parquet").option("header", "true").save(Constants.HDFS_HBASE_QUERY1);
        //resultsDS.write().format("csv").option("header", "true").save(Constants.HDFS_HBASE_QUERY1);
        //resultsDS.write().format("json").option("header", "true").save(Constants.HDFS_MONGO_QUERY1);


//stop time
       //startTime


//stop time

        //startTime

        //SQLQuery1.executeQuery(spark,data);

//stop time

        spark.stop();

    }

    public static void main(String[] args) {

        MainQuery1.executeMain();
    }

}
