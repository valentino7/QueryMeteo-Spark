package first_query;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;

public class  Query1Preprocessing {

    private static String pathToFile = "data/weather_description.csv";

    public static JavaRDD<Record> preprocessDataset(JavaSparkContext sc) {


        JavaRDD<String> weatherFile = sc.textFile(pathToFile);
        JavaRDD<Record> records =
                weatherFile.map(
                        // line -> OutletParser.parseJson(line))         // JSON
                        line -> RecordParser.parseCSV(line))            // CSV
                        .filter(x -> x != null );
        return records;



    }
}