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
import sparkCore.Query2;
import sparkSQL.SQLQuery2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ControllerQuery2 {


    public static Dataset<Row> convertToDataset(SparkSession spark, JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple4<Double, Double ,Double, Double>> result){
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(Constants.COUNTRY_LABEL, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.YEAR_LABEL, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.MONTH_LABEL, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.AVERAGE_LABEL, DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField(Constants.MIN_LABEL, DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField(Constants.MAX_LABEL, DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField(Constants.STDDEV_LABEL, DataTypes.DoubleType, false));
        StructType schemata = DataTypes.createStructType(fields);


        JavaRDD<Row> rows = result.map(t -> RowFactory.create(t._1._3(), t._1._1(),t._1._2(), t._2._1(),t._2._3(),t._2._4(),t._2._2()  ));
        return spark.sqlContext().createDataFrame(rows, schemata);
    }

    public static void executeMain(String HDFS_ROOT,String mode){

        SparkSession spark = Session.getSession(Constants.QUERY2_NAME,mode);

        Dataset<Row> city_file = spark.read().option(Constants.HEADER_OPTION,Constants.HEADER_BOOL).csv(HDFS_ROOT +Constants.HDFS_INPUT+Constants.CITY_FILE_CSV);
        Map<String, Tuple2<String,String>> country = Nations.getNation(city_file);


        for (int i =0; i < Constants.STATISTICS_FILE; i++) {

            Dataset<Row> inputData =  spark.read().option(Constants.HEADER_OPTION,Constants.HEADER_BOOL).csv(HDFS_ROOT+Constants.HDFS_INPUT +Constants.FILE[i]);

            JavaRDD<Tuple3<String, String, Double>> values = AllQueryPreProcess.executePreProcess(inputData,  2);

            // (Year,Month,Nation) , (Value, count)
            JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double, Double>> dt = Query2Preprocess.executeProcess(country, values, i);

            //JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple4<Double, Double ,Double, Double>> result = Query2.executeQuery(dt);
            Dataset<Row> res = convertToDataset(spark, Query2.executeQuery(dt));

            //resultsDS.write().format("parquet").option("header", "true").save(Constants.HDFS_HBASE_QUERY1);
            //resultsDS.write().format("csv").option("header", "true").save(Constants.HDFS_HBASE_QUERY1);

            String folderName= "";
            switch (i){
                case 0:
                    folderName = "temperature";
                    break;
                case 1:
                    folderName = "pressure";
                    break;
                case 2:
                    folderName = "humidity";
                    break;
            }

            res.coalesce(1).write().format(Constants.JSON_FORMAT).option(Constants.HEADER_OPTION,Constants.HEADER_BOOL).save(HDFS_ROOT+ Constants.HDFS_MONGO_QUERY2+folderName);


            Dataset<Row> resultSQL = SQLQuery2.executeQuery(spark,dt);

            resultSQL.coalesce(1).write().format(Constants.JSON_FORMAT).option(Constants.HEADER_OPTION,Constants.HEADER_BOOL).save(HDFS_ROOT+ Constants.HDFS_MONGO_QUERY2_SQL+ (i+7));

        }

        spark.stop();
    }

    public static void main(String[] args) {

        String HDFS_ROOT = "hdfs://"+ args[0]+"/";
        ControllerQuery2.executeMain(HDFS_ROOT, args[1]);

    }
}
