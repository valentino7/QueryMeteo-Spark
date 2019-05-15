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

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

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
        //fields.add(DataTypes.createStructField("count", DataTypes.DoubleType, true));

        StructType schema = DataTypes.createStructType(fields);


        JavaRDD<Row> rows = values.map(new Function<Tuple2<Tuple5<Integer, Integer, Integer, String,String>, Tuple2<Double,Double>>, Row>() {
            @Override
            public Row call(Tuple2<Tuple5<Integer, Integer, Integer, String,String>, Tuple2<Double,Double>> tuple) throws Exception {
                return RowFactory.create(tuple._1()._1(), tuple._1()._2(), tuple._1()._3(), tuple._1()._4(), tuple._1()._5(), tuple._2()._1(), tuple._2()._2());
            }
        });

        Dataset<Row> df = spark.createDataFrame(rows, schema);



        df.createOrReplaceTempView("filteredTab");
        /*
            anni 2016-2017 per i due quadrimestri giugno-settembre e gennaio-aprile
            nella fascia oraria 12:00-15:00
        */
        Dataset<Row> filteredTable = spark.sql("" +
                "SELECT * " +
                "FROM filteredTab " +
                "WHERE year >= 2016 AND ( (month >= 6 AND month <= 9) " +
                "OR (month >= 1 AND month <= 4) ) AND (hour >= 12 AND hour <= 15)");

        //filteredTable.show(50);

        filteredTable.createOrReplaceTempView("avgTemp1");

        //media calcolata a mano
       /* Dataset<Row> sumTable1 = spark.sql("" +
                "SELECT *" +
                "FROM (SELECT year, nation, city, count(month) as count_month, SUM(temperature) as sum_temp " +
                        "FROM avgTemp1 " +
                        "WHERE month >= 1 AND month <= 4 " +
                        "GROUP BY year, nation, city) " +
                "GROUP BY year, nation, city, sum_temp, count_month");

        sumTable1.show();*/

        //calcolo della media delle temperature per il primo quadrimestre
        Dataset<Row> avgTable1 = spark.sql("" +
                "SELECT year, nation, city, AVG(temperature) as avg_temp1 " +
                "FROM avgTemp1 " +
                "WHERE month >= 1 AND month <= 4 " +
                "GROUP BY year, nation, city");


        //media calcolata a mano
        /*Dataset<Row> avgTable2 = spark.sql("" +
                "SELECT year, nation, city, (sum_temp/count_month) as avg_temp2 " +
                "FROM (SELECT year, nation, city, count(month) as count_month, SUM(temperature) as sum_temp " +
                        "FROM avgTemp2 " +
                        "WHERE month >= 6 AND month <= 9 " +
                        "GROUP BY year, nation, city) " +
                "GROUP BY year, nation, city, avg_temp2");*/

        filteredTable.createOrReplaceTempView("avgTemp2");

        //calcolo della media delle temperature per il secondo quadrimestre
        Dataset<Row> avgTable2 =spark.sql("" +
                "SELECT year, nation, city, AVG(temperature) as avg_temp2 " +
                "FROM avgTemp2 " +
                "WHERE month >= 6 AND month <= 9 " +
                "GROUP BY year, nation, city");

        avgTable2.show(50);


        avgTable1.createOrReplaceTempView("temp1");
        avgTable2.createOrReplaceTempView("temp2");


        /*
            differenza delle temperature dalla massima alla minima
            per ogni nazione-città
         */
        Dataset<Row> joinTable = spark.sql("" +
                "SELECT year, nation, city, sub_temp " +
                "FROM (SELECT t1.year, t1.nation, t1.city, ABS(t1.avg_temp1 - t2.avg_temp2) as sub_temp " +
                        "FROM temp1 as t1 JOIN temp2 as t2 " +
                        "ON t1.year = t2.year AND t1.city = t2.city " +
                        "GROUP BY t1.year, t1.nation, t1.city, sub_temp) " +
                "GROUP BY year, nation, city, sub_temp " +
                "ORDER BY sub_temp DESC");


        joinTable.createOrReplaceTempView("filter");
        Dataset<Row> p2016 = spark.sql( "SELECT * " +
                "FROM filter " +
                "WHERE year == 2016");


        Dataset<Row> p2017 = spark.sql( "SELECT * " +
                "FROM filter " +
                "WHERE year == 2017");

         p2016.show(100);
         p2017.show(100);
        //TODO: organizzare due tabelle una per 2017 e una per 2016 con rank
    }
}
