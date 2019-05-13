import Utils.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import sparkSQL.SQLQuery1;
import sparkSQL.SQLQuery2;
import sparkSQL.SQLQuery3;
import spark_v2.Query1_v2;
import spark_v2.Query2_v2;
import spark_v2.Query3_v2;


public class Main_v2 {

    public static void main(String[] args) {

        if (args.length < 1 ){
            System.err.println("Usage: Main <queryNumber(1,2,3)> <isSql(True,False)> <params>");
            System.exit(1);
        }
        int queryNumber = Integer.parseInt(args[0]);
        boolean isSQL = Boolean.parseBoolean(args[1]);





        switch (queryNumber){

            case 1 :


                JavaSparkContext sc= Context.getContext("Query1");
                JavaRDD<Tuple3<String,String,Double>>values = AllQueryPreProcess.executePreProcess(sc,Constants.WEATHER_FILE,1);
                JavaPairRDD<Tuple4<Integer, Integer, Integer, String>, Double> data = Query1Preprocess.executeProcess(values);


                if (isSQL){

                    SparkSession spark = SparkSession
                            .builder()
                            .appName("Java Spark SQL query1").master("local")
                            //.config("spark.some.config.option", "some-value")
                            .getOrCreate();


                    SQLQuery1.executeQuery(spark,data);

                }else {
                    long time = EvaluateTime.getTime();

                    Query1_v2.executeQuery(data);

                    time = ( EvaluateTime.getTime() - time ) / (long)Math.pow(10,9);

                    System.out.println(time);

                }
                sc.stop();

                break;


            case 2:

                if (isSQL){
                    SQLQuery2.executeQuery(args);
                }else {
                    for (int i = 0 ; i < Constants.STATISTICS_FILE; i++){

                        JavaSparkContext sc2= Context.getContext("Query2");
                        JavaRDD<Tuple3<String,String,Double>> valuesq2 = AllQueryPreProcess.executePreProcess(sc2,Constants.FILE[i],2);

                        // (Year,Month,Nation) , (Value, count)
                        JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double,Double> > dt =  Query2Preprocess.executeProcess(sc2,valuesq2,i);

                        Query2_v2.executeQuery( dt, i );

                        sc2.stop();
                    }

                }

                break;

            case 3 :

                if (isSQL){
                    SQLQuery3.executeQuery(args);
                }else {

                    JavaSparkContext sc3= Context.getContext("Query3");

                    JavaRDD<Tuple3<String,String,Double>> valuesq3 = AllQueryPreProcess.executePreProcess(sc3,Constants.TEMPERATURE_FILE,3);

                    JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> preprocess = Query3Preprocess.executeProcess(sc3,valuesq3);

                    Query3_v2.executeQuery(preprocess);

                    sc3.stop();


                   // Query3.executeQuery(args);
                }

                break;

            default:
                System.out.println("Exit");
                System.exit(0);
        }

    }
}
