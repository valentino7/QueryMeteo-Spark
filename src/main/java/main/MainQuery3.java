package main;

import Utils.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

import sparkSQL.SQLQuery3;
import spark.Query3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MainQuery3 {


    public static Dataset<Row> convertToDataset(SparkSession spark, JavaPairRDD<String, List<Tuple2<String,Integer> >>  result){
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("country", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("city", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("currentYear", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("currentPosition", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("LastYear", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("LastPosition", DataTypes.IntegerType, false));
        StructType schemata = DataTypes.createStructType(fields);


        JavaRDD<Row> rows = result
                .flatMap(new FlatMapFunction<Tuple2<String, List<Tuple2<String, Integer>>>, Row>() {
                    @Override
                    public Iterator<Row> call(Tuple2<String, List<Tuple2<String, Integer>>> t) throws Exception {
                        List<Row> listToreturn = new ArrayList<>();
                        List<Tuple2<String,Integer>> temp = t._2;
                        for ( int i = 0; i< temp.size(); i++ ){
                            Row r = RowFactory.create(t._1(),temp.get(i)._1,2017,i+1,2016,temp.get(i)._2  );
                            listToreturn.add(r);
                        }
                      return listToreturn.iterator();

                    }
                });
        return spark.sqlContext().createDataFrame(rows, schemata);
    }

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
        JavaPairRDD<String, List<Tuple2<String,Integer> >> result = Query3.executeQuery(preprocess);
        //getTIme

        Dataset<Row> res = convertToDataset(spark,result);

        res.coalesce(1).write().format("json").option("header", "true").save(Constants.HDFS_MONGO_QUERY3);

        SQLQuery3.executeQuery(spark,preprocess);


        spark.stop();
    }
}
