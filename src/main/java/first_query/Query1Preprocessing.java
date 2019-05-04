package first_query;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;


public class  Query1Preprocessing {

    private static String pathToFile = "data/weather_description.csv";

    public static JavaRDD<Record> preprocessDataset(JavaSparkContext sc) {


        // da rivedere
        JavaRDD<String> rawInputFile = sc.textFile(pathToFile);

        Function2 takeHeader= new Function2<Integer, Iterator<String>, Iterator<Record>>(){
            @Override
            public Iterator<Record> call(Integer ind, Iterator<String> iterator) throws Exception {
                if(ind==0 && iterator.hasNext()){

                    //parser città
                    RecordParser p = new RecordParser(iterator.next());
                    iterator.next();

                    //parser meteo
                    ArrayList<Record> rdd=new ArrayList<>();
                    while(iterator.hasNext()){
                        rdd.add(p.parseCSV(iterator.next()));
                    }

                    return rdd.iterator();
                }
                return null;
            }
        };
        JavaRDD<Record> inputRdd = rawInputFile.mapPartitionsWithIndex(takeHeader, false);

        inputRdd.collect();
        //stampo solo la data per controllare il contenuto
        for(Record r: inputRdd.collect()) {
            System.out.println(r.getDate() + "\n");

            break;
        }

        return inputRdd;



    }
}