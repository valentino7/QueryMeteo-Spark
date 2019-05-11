package Utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class AllQueryPreProcess {
    public static JavaRDD<Tuple3<String,String,Double>> executePreProcess(JavaSparkContext sc, String file, int queryNumber) {

        JavaRDD<String> csvFile = sc.textFile(file);
        String firstLine = csvFile
                .first();

        List<String> citiesArray = new ArrayList<>(Arrays.asList(firstLine.split(",")));
        citiesArray.remove(0);


        return csvFile
                .filter(csvLine -> !csvLine.equals(firstLine))
                .flatMap(new FlatMapFunction<String, Tuple3<String, String, Double>>() {
                    @Override
                    public Iterator<Tuple3<String, String, Double>> call(String s) throws Exception {
                        String[] strings = s.split(",", -1);
                       /* DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = df.parse(strings[0]);
                        GregorianCalendar cal = new GregorianCalendar();
                        cal.setTime(date);*/
                        List<Tuple3<String, String, Double>> list = new ArrayList<>();
                        for (int i = 1; i < strings.length; i++) {
                            if ( queryNumber == 1 ) {
                                list.add(new Tuple3<>(strings[0], citiesArray.get(i - 1), strings[i].equals("sky is clear") ? 1.0 : 0.0));
                            }else {
                                list.add(new Tuple3<>(strings[0], citiesArray.get(i - 1), strings[i].isEmpty() ?  0.0 : Double.parseDouble(strings[i])));
                            }
                        }

                        return list.iterator();
                    }
                });

    }
}
