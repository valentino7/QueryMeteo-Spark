package Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class AllQueryPreProcess {

    public static JavaRDD<Tuple3<String,String,Double>> executePreProcess(Dataset<Row> inputData ,int queryNumber) {


        /*

        take header from dataset ( list of city )

         */
        List<String> citiesArray= new ArrayList<>(Arrays.asList(inputData.columns()));
        citiesArray.remove(0);


        /*

        .flatMap:


         */

        return inputData
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, Tuple3<String, String, Double>>) s -> {
                    List<Tuple3<String, String, Double>> list = new ArrayList<>();
                    for (int i = 1; i < s.size(); i++) {
                        String city = citiesArray.get(i - 1).replaceAll("_"," ");
                        if ( queryNumber == 1 ) {
                            list.add(new Tuple3<>(s.getString(0), city, ( s.isNullAt(i) ||  !s.getString(i).equals(Constants.WEATHER) ) ? 0.0 : 1.0));
                        }else {
                            list.add(new Tuple3<>( s.getString(0) , city , ( s.isNullAt(i) || s.getString(i).isEmpty() ) ?    0.0 : Double.parseDouble(s.getString(i)) ) );
                        }
                    }

                    return list.iterator();
                })
                .cache();

    }


}
