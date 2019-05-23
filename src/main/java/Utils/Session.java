package Utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Session {

    public static SparkSession getSession(String name, String mode){

        // SparkContext creation
        //startTimer
        SparkSession spark = null;

        switch (mode){
            case "cluster":
                spark = SparkSession
                        .builder()
                        .appName(name)
                        //.master(Constants.MASTER)
                        //.config("sparkCore.some.config.option", "some-value")
                        .getOrCreate();

                break;
            case "local":
                spark = SparkSession
                        .builder()
                        .appName(name)
                        .master(Constants.MASTER)
                        //.config("sparkCore.some.config.option", "some-value")
                        .getOrCreate();

                break;
        }

        assert (spark!=null);
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.hadoopConfiguration().set(Constants.SPAK_SUCCESS_CONF,Constants.SPAK_SUCCESS_BOOL);

        return spark;
    }

}
