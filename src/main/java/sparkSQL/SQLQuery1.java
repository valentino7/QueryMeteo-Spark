package sparkSQL;


import Utils.EvaluateTime;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple4;
import java.util.*;



public class SQLQuery1 {

    private static String inputPath = "data/city_attributes.csv";
    private static String inputPath2 = "data/weather_description.csv";

    public static void executeQuery(SparkSession spark,JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> values) {


        //Dataset df = spark.read().format("csv").option("header", "true").load(inputPath2);

        //creo lo schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("year", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("month", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("day", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("cities", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("count", DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(fields);


        long time = EvaluateTime.getTime();

        JavaRDD<Row> rows = values.map(new Function<Tuple2<Tuple4<Integer, Integer, Integer, String>, Double>, Row>() {
            @Override
            public Row call(Tuple2<Tuple4<Integer, Integer, Integer, String>, Double> tuple) throws Exception {
                return RowFactory.create(tuple._1._1(), tuple._1._2(), tuple._1._3(), tuple._1._4(), tuple._2);
            }
        });

        Dataset<Row> df = spark.createDataFrame(rows, schema);


        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("clearSky");
        //conteggio del cielo sereno per ogni città, per ogni giorno, per ogni mese
        Dataset<Row> clearSky = spark.sql(
                "SELECT year, month, day, cities, SUM(count) AS sum " +
                        "FROM clearSky  " +
                        "GROUP BY year, month, day, cities");
        //clearSky.sort("year", "month", "day").show();

        clearSky.createOrReplaceTempView("tmp");
        //filter delle città con più di 18 ore di cielo sereno
        Dataset<Row> tmpResult = spark.sql(
                "SELECT year, month, day, cities, sum " +
                        "FROM tmp WHERE sum >= 18 " +
                        "GROUP BY year, month, day, cities, sum");
        //tmpResult.sort("year", "month", "day").show(50);

        tmpResult.createOrReplaceTempView("tmp2");
        //città con almeno 15 giorni al mese di cielo sereno
        Dataset<Row> clearSkyDays = spark.sql(
                 "SELECT year, month , cities, numdays " +
                        "FROM (SELECT year, month, cities, COUNT(day) as numdays " +
                                "FROM tmp2 " +
                                "GROUP BY year, month, cities) " +
                        "WHERE numdays >= 15 " +
                        "ORDER BY year");
        //clearSkyDays.show(50);


        clearSkyDays.createOrReplaceTempView("finaleView");
        Dataset<Row> result = spark.sql(
                    "SELECT year, cities " +
                           "FROM (SELECT year, COUNT(month) as countmonth, cities " +
                                "FROM finaleView " +
                                "GROUP BY year, cities " +
                                "ORDER BY year) " +
                           "WHERE countmonth == 3 " +
                           "ORDER BY year");

        result.show();

        time = ( EvaluateTime.getTime() - time ) / (long)Math.pow(10,9);

        System.out.println(time);



        spark.stop();
    }

}
