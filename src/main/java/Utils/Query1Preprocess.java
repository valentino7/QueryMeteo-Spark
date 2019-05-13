package Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class Query1Preprocess {

    public static JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> executeProcess(JavaRDD<Tuple3<String,String,Double>> values) {

        return values
                .mapToPair(new PairFunction<Tuple3<String, String, Double>, Tuple4<Integer,Integer,Integer,String>, Double>() {
                    @Override
                    public Tuple2<Tuple4<Integer, Integer, Integer, String>, Double> call(Tuple3<String, String, Double> tuple) throws Exception {
                        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = df.parse(tuple._1());
                        GregorianCalendar cal = new GregorianCalendar();
                        cal.setTime(date);
                        cal.setTimeZone(TimeZone.getTimeZone("UTC"));

                        return new Tuple2<>(new Tuple4<>(cal.get(Calendar.YEAR),cal.get(Calendar.MONTH),cal.get(Calendar.DAY_OF_MONTH),tuple._2()),tuple._3());
                    }
                })
                .filter( object -> (
                        object._1._2() == 2 ||
                                object._1._2() == 3 ||
                                object._1._2() == 4 ) );
    }

}
