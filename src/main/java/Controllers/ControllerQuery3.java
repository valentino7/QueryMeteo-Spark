package Controllers;

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
import sparkCore.Query3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ControllerQuery3 {


    public static Dataset<Row> convertToDataset(SparkSession spark, JavaPairRDD<String, List<Tuple2<String,Integer> >>  result){
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(Constants.COUNTRY_LABEL, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CITY_LABEL, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CURRENTYEAR_LABEL, DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField(Constants.CURRENTPOSITION_LABEL, DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField(Constants.LASTYEAR_LABEL, DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField(Constants.LASTPOSITION_LABEL, DataTypes.IntegerType, false));
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

    public static void executeMain(String HDFS_ROOT){

        SparkSession spark = Context.getSession(Constants.QUERY3_NAME);

        Dataset<Row> inputData = spark.read().option(Constants.HEADER_OPTION,Constants.HEADER_BOOL).csv(HDFS_ROOT+Constants.HDFS_INPUT +Constants.TEMPERATURE_FILE_CSV);
        Dataset<Row> city_file = spark.read().option(Constants.HEADER_OPTION,Constants.HEADER_BOOL).csv(HDFS_ROOT+Constants.HDFS_INPUT+Constants.CITY_FILE_CSV);

        //Nations
        Map<String, Tuple2<String,String>> country = Nations.getNation(city_file);
        JavaRDD<Tuple3<String,String,Double>> valuesq3 = AllQueryPreProcess.executePreProcess(inputData,3);
        JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> preprocess = Query3Preprocess.executeProcess(country,valuesq3);

        //getTIme
        JavaPairRDD<String, List<Tuple2<String,Integer> >> result = Query3.executeQuery(preprocess);
        //getTIme

        Dataset<Row> res = convertToDataset(spark,result);
        res.coalesce(1).write().format(Constants.JSON_FORMAT).option(Constants.HEADER_OPTION,Constants.HEADER_BOOL).save(HDFS_ROOT+Constants.HDFS_MONGO_QUERY3);

        Dataset<Row> resultSQL = SQLQuery3.executeQuery(spark,preprocess);
        resultSQL.coalesce(1).write().format(Constants.JSON_FORMAT).option(Constants.HEADER_OPTION,Constants.HEADER_BOOL).save(HDFS_ROOT+Constants.HDFS_MONGO_QUERY3_SQL);

        spark.stop();
    }

    public static void main(String[] args) {
        String HDFS_ROOT = "hdfs://"+ args[0]+"/";
        ControllerQuery3.executeMain(HDFS_ROOT);

    }
}
