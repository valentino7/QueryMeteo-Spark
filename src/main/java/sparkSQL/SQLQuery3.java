package sparkSQL;


import Utils.Constants;
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
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;

public class SQLQuery3 {


    public static Dataset<Row> executeQuery(SparkSession spark, JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> values) {


        //creo lo schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(Constants.YEAR_LABEL, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.MONTH_LABEL, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.HOUR_LABEL, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.COUNTRY_LABEL, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CITY_LABEL, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.TEMPERATURE_LABEL, DataTypes.DoubleType, true));
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


        //calcolo della media delle temperature per il primo quadrimestre
        Dataset<Row> avgTable1 = spark.sql("" +
                "SELECT year, country, city, AVG(temperature) as avg_temp1 " +
                "FROM avgTemp1 " +
                "WHERE month >= 1 AND month <= 4 " +
                "GROUP BY year, country, city");



        filteredTable.createOrReplaceTempView("avgTemp2");

        //calcolo della media delle temperature per il secondo quadrimestre
        Dataset<Row> avgTable2 =spark.sql("" +
                "SELECT year, country, city, AVG(temperature) as avg_temp2 " +
                "FROM avgTemp2 " +
                "WHERE month >= 6 AND month <= 9 " +
                "GROUP BY year, country, city");

        //avgTable2.show(50);


        avgTable1.createOrReplaceTempView("temp1");
        avgTable2.createOrReplaceTempView("temp2");


        /*
            differenza delle temperature dalla massima alla minima
            per ogni nazione-città
         */
        Dataset<Row> joinTable = spark.sql("" +
                "SELECT year, country, city, sub_temp " +
                "FROM (SELECT t1.year, t1.country, t1.city, ABS(t1.avg_temp1 - t2.avg_temp2) as sub_temp " +
                        "FROM temp1 as t1 JOIN temp2 as t2 " +
                        "ON t1.year = t2.year AND t1.city = t2.city " +
                        "GROUP BY t1.year, t1.country, t1.city, sub_temp) " +
                "GROUP BY year, country, city, sub_temp " +
                "ORDER BY sub_temp DESC");


        joinTable.createOrReplaceTempView("filter");

        //classifica delle città per nazione nel 2016
        Dataset<Row> rank2016 = spark.sql(
                "SELECT year, country, city, sub_temp, " +
                "DENSE_RANK() OVER (PARTITION BY country ORDER BY sub_temp DESC) as rank " +
                "FROM filter " +
                "WHERE (year == 2016) " +
                "GROUP BY year, country, city, sub_temp");


        //top 3 città per ogni nazione nel 2017
        Dataset<Row> topThree2017 = spark.sql(
        "SELECT year, country, city, sub_temp, rank " +
               "FROM (SELECT *, " +
                        "DENSE_RANK() OVER (PARTITION BY country ORDER BY sub_temp DESC) as rank " +
                        "FROM filter " +
                        "WHERE year == 2017) " +
               "WHERE rank BETWEEN 1 AND 3 " +
               "GROUP BY year, country, city, sub_temp, rank");


        //rank2016.show(50);
        //topThree2017.show();


        rank2016.createOrReplaceTempView("rank2016");
        topThree2017.createOrReplaceTempView("rank2017");


        //confronto tra rank delle città nel 2017 e nel 2016
        return spark.sql(
                "SELECT r1.country, r1.city, r2.year as currentYear, r2.rank as currentPosition, r1.year as lastYear, r1.rank as LastPosition " +
                        "FROM rank2016 as r1 JOIN rank2017 as r2 " +
                        "ON r1.country == r2.country AND r1.city == r2.city " +
                        "GROUP BY r1.year, r2.year, r1.country, r2.country, r1.city, r2.city, r1.rank, r2.rank, r1.sub_temp, r2.sub_temp " +
                        "ORDER BY r1.country, r2.rank");

        //compareRanks.show();


    }
}
