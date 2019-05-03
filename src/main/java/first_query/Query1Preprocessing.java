package first_query;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;


public class  Query1Preprocessing {

    private static String pathToFile = "data/weather_description.csv";

    public static JavaRDD<Record> preprocessDataset(JavaSparkContext sc) {


        // da rivedere
        JavaRDD<String> rawInputFile = sc.textFile(pathToFile);

        Function2 takeHeader= new Function2<Integer, Iterator<String>, Iterator<String>>(){
            @Override
            public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
                if(ind==0 && iterator.hasNext()){
                    iterator.next();
                    System.out.println("sono dentro id = " + ind );
                    return iterator;
                }else {
                    return iterator;
                }
            }
        };
        JavaRDD<String> inputRdd = rawInputFile.mapPartitionsWithIndex(takeHeader, false);

        // da rivedere
        inputRdd.collect();

        JavaRDD<String> weatherFile = sc.textFile(pathToFile);

        /*for(String r: weatherFile.collect()) {
            System.out.println(r + "\n");
        }*/

        JavaRDD<Record> records = weatherFile.map( line -> RecordParser.parseCSV(line));

        return records;



    }
}