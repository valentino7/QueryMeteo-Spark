package Controllers;

import Utils.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
import sparkSQL.SQLQuery1;
import sparkCore.Query1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ControllerQuery1 {


    public static Dataset<Row> convertToDataset(SparkSession spark,JavaPairRDD<Integer, String> result){
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("year", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("cities", DataTypes.StringType, true));
        StructType schemata = DataTypes.createStructType(fields);


        JavaRDD<Row> rows = result.map(t -> {
            return RowFactory.create(t._1(), t._2()  );
        });
        return spark.sqlContext().createDataFrame(rows, schemata);
    }

    public static void executeMain(String HDFS_ROOT) {

        //startTimer
        SparkSession spark = Context.getContext("query1");

        /*
        inputData = sparkCore.read().parquet(Constants.HDFS_INPUT +Constants.WEATHER_FILE);
        inputData = sparkCore.read().csv(Constants.HDFS_INPUT +Constants.WEATHER_FILE);
        */

        Dataset<Row> inputData = spark.read().option("header","true").csv(HDFS_ROOT+Constants.HDFS_INPUT +Constants.WEATHER_FILE_CSV);
        Dataset<Row> city_file = spark.read().option("header","true").csv(HDFS_ROOT+Constants.HDFS_INPUT +Constants.CITY_FILE_CSV);

        //Nations
        Map<String, Tuple2<String,String>> country = Nations.getNation(spark, city_file);

        JavaRDD<Tuple3<String,String,Double>> values = AllQueryPreProcess.executePreProcess(inputData,1).cache();

        JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> data = Query1Preprocess.executeProcess(country,values).cache();

        JavaPairRDD<Integer, String> result = Query1.executeQuery(data);

        Dataset<Row> resultsDS= convertToDataset(spark,result);
        //resultsDS.show(100);
        //resultsDS.write().format("parquet").option("header", "true").save(Constants.HDFS_HBASE_QUERY1);
        //resultsDS.write().format("csv").option("header", "true").save(Constants.HDFS_HBASE_QUERY1);
        resultsDS.coalesce(1).write().format("json").option("header", "true").save(HDFS_ROOT+Constants.HDFS_MONGO_QUERY1);


        Dataset<Row> resultSQL = SQLQuery1.executeQuery(spark,data);
        resultSQL.coalesce(1).write().format("json").option("header", "true").save(HDFS_ROOT+Constants.HDFS_MONGO_QUERY1_SQL);



        spark.stop();

    }

    public static void main(String[] args) {

        String HDFS_ROOT = "hdfs://"+ args[0]+"/";
        ControllerQuery1.executeMain(HDFS_ROOT);
    }

}
