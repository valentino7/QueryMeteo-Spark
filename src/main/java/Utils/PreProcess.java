package Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.geonames.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PreProcess {

    public static Map<String, String> executeProcess(JavaSparkContext sc) {

        JavaRDD<String> city_attributes = sc.textFile(Constants.CITY_FILE);
        String firstLine = city_attributes.first();
        JavaPairRDD<String, String> city_nations = city_attributes
                .filter(v1 -> !v1.equals(firstLine))
                .mapToPair(PreProcess::findNation);
        return city_nations.collectAsMap();
    }

    private static Tuple2<String, String> findNation(String s) {

        // funzione per prendere la nazione tramite la stringa : chiamata ad un API oppure ricerca in un file in locale

        String[] arrays = s.split(",");
        String city = arrays[0];
        String lat = arrays[1];
        String lon = arrays[2];

        WebService.setUserName("tripps");
        WebService.setConnectTimeOut(100000000);
        List<Toponym> result = new ArrayList<>();

        try {

            result = WebService.findNearbyPlaceName(Double.parseDouble(lat),Double.parseDouble(lon));

        } catch (Exception e) {
            e.printStackTrace();
        }


        return new Tuple2<>(city,result.get(0).getCountryName());

    }


}
