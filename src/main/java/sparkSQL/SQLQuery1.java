package sparkSQL;

import Utils.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple4;
import scala.collection.immutable.Seq;

import javax.xml.crypto.Data;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.spark.sql.functions.col;

public class SQLQuery1 {

    private static String inputPath = "data/city_attributes.csv";
    private static String inputPath2 = "data/weather_description.csv";

    public static void executeQuery(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query1").master("local")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();



        //Dataset df = spark.read().format("csv").option("header", "true").load(inputPath2);

        //preprocessing per rendere il dataset più facile da usare per sparksql
        JavaRDD<String> weatherFile = sc.textFile(Constants.WEATHER_FILE);
        String firstLine = weatherFile.first();
        List<String> citiesArray = new ArrayList<>(Arrays.asList(firstLine.split(",")));
        citiesArray.remove(0);

        //TODO: codice ripetuto da risolvere
        JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Integer> citiesWithClearSky = weatherFile
                .filter(csvLine -> !csvLine.equals(firstLine))
                .flatMapToPair((PairFlatMapFunction<String, Tuple4<Integer, Integer, Integer, String>, Integer>) s -> {
                    String[] strings = s.split(",", -1);
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = df.parse(strings[0]);
                    GregorianCalendar cal = new GregorianCalendar();
                    cal.setTime(date);
                    List<Tuple2<Tuple4<Integer, Integer, Integer, String>, Integer>> list = new ArrayList<>();
                    for (int i = 1; i < strings.length; i++) {
                        list.add(new Tuple2<>(new Tuple4<>(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), citiesArray.get(i - 1)), strings[i].equals("sky is clear") ? 1 : 0));
                    }
                    return list.iterator();

                })
                .filter(object -> (
                        object._1._2() == 2 ||
                                object._1._2() == 3 ||
                                object._1._2() == 4));


        //creo lo schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("year", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("month", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("day", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("cities", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("count", DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rows = citiesWithClearSky.map(new Function<Tuple2<Tuple4<Integer, Integer, Integer, String>, Integer>, Row>() {
            @Override
            public Row call(Tuple2<Tuple4<Integer, Integer, Integer, String>, Integer> tuple) throws Exception {
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
        //result.sort("year", "month", "day").show();

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
                 "SELECT year, month, cities, numdays " +
                        "FROM (SELECT year, month, cities, COUNT(day) as numdays " +
                                "FROM tmp2 " +
                                "GROUP BY year, month, cities) " +
                        "WHERE numdays >= 15 " +
                        "ORDER BY year, month");
        //clearSkyDays.show(50);

        //TODO: non funziona
        clearSkyDays.createOrReplaceTempView("finaleView");
        Dataset<Row> result = spark.sql(
                    "SELECT f1.year, f1.cities " +
                            "FROM finaleView as f1 INNER JOIN finaleView as f2 " +
                            "ON f1.month = f2.month " +
                            "ORDER BY f1.year, f1.month");

        result.show(50);

        //df.sort("year", "month", "day").show(100);

        spark.stop();
    }

}
