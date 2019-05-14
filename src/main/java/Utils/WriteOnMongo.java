package Utils;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple3;

public class WriteOnMongo {

    public static void write(JavaSparkContext sc, JavaPairRDD<Integer, Iterable<String>> result){
        // Create a RDD of 10 documents
        JavaRDD<Document> sparkDocuments = result
                .map(new Function<Tuple2<Integer, Iterable<String>>, Document>() {
                    @Override
                    public Document call(Tuple2<Integer, Iterable<String>> v1) throws Exception {
                        Document doc = new Document();
                        doc.put("Year",v1._1().toString());
                        doc.put("Cities", v1._2() );
                        return doc;
                    }
                });

        MongoSpark.save(sparkDocuments);

    }
}
