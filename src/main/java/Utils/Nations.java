package Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.geonames.Timezone;
import org.geonames.WebService;
import scala.Tuple2;

import java.util.Map;

public class Nations {

    public static Map<String, Tuple2<String,String>> getNation(Dataset<Row> inputData) {

        JavaPairRDD<String, Tuple2<String, String>> city_nations = inputData
                .toJavaRDD()
                .mapToPair(Nations::findNation);

        return city_nations.collectAsMap();
    }

    public static Tuple2<String, Tuple2<String, String> > findNation (Row row){

        String city = row.getString(0);
        String lat = row.getString(1);
        String lon = row.getString(2);

        WebService.setUserName(Constants.GEONAMES_USERNAME);
        Timezone time = null;
        try {
            time = WebService.timezone(Double.parseDouble(lat),Double.parseDouble(lon));

        } catch (Exception e) {
            e.printStackTrace();
        }

        assert time != null;
        return new Tuple2<>(city,new Tuple2<>(time.getCountryCode(),time.getTimezoneId()) );

    }

}
