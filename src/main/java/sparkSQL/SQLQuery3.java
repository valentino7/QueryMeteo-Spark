package sparkSQL;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;

public class SQLQuery3 {


    public static void executeQuery(SparkSession spark, JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> values) {


        //creo lo schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("year", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("month", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("hour", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("nation", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("city", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("temperature", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("count", DataTypes.DoubleType, true));

        StructType schema = DataTypes.createStructType(fields);


        JavaRDD<Row> rows = values.map(new Function<Tuple2<Tuple5<Integer, Integer, Integer, String,String>, Tuple2<Double,Double>>, Row>() {
            @Override
            public Row call(Tuple2<Tuple5<Integer, Integer, Integer, String,String>, Tuple2<Double,Double>> tuple) throws Exception {
                return RowFactory.create(tuple._1()._1(), tuple._1()._2(), tuple._1()._3(), tuple._1()._4(), tuple._1()._5(), tuple._2()._1(), tuple._2()._2());
            }
        });

        Dataset<Row> df = spark.createDataFrame(rows, schema);

        df.show(100);
        df.createOrReplaceTempView("clearSky");


    }
}
