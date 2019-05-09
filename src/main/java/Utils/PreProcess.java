package Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PreProcess {

    public static void executeProcess(JavaSparkContext sc) {

        JavaRDD<String> city_attributes = sc.textFile(Constants.CITY_FILE);
        JavaPairRDD<String, String> city_nations = city_attributes.mapToPair(s -> findNation(s));

        city_nations.cache();
    }

    private static Tuple2<String, String> findNation(String s) {

        String state = null;
        // funzione per prendere la nazione tramite la stringa : chiamata ad un API oppure ricerca in un file in locale

        return new Tuple2<>(s,state);

    }


}
