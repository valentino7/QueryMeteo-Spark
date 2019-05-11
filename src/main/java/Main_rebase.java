import Utils.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import spark.Query3;
import sparkSQL.SQLQuery1;
import sparkSQL.SQLQuery2;
import sparkSQL.SQLQuery3;
import spark_v2.Query1_v2;
import spark_v2.Query2_v2;
import spark_v2.Query3_v2;

import java.util.GregorianCalendar;
import java.util.Map;

public class Main_rebase {

    public static void main(String[] args) {

        if (args.length < 1 ){
            System.err.println("Usage: Main <queryNumber(1,2,3)> <isSql(True,False)> <params>");
            System.exit(1);
        }
        int queryNumber = Integer.parseInt(args[0]);
        boolean isSQL = Boolean.parseBoolean(args[1]);

        switch (queryNumber){

            case 1 :



                if (isSQL){
                    SQLQuery1.executeQuery(args);
                }else {
                    JavaSparkContext sc= Context.getContext("Query1");
                    JavaRDD<Tuple3<String,String,Double>>values = AllQueryPreProcess.executePreProcess(sc,Constants.WEATHER_FILE,1);

                    Query1_v2.executeQuery(values);
                    sc.stop();
                }

                break;


            case 2:

                if (isSQL){
                    SQLQuery2.executeQuery(args);
                }else {
                    for (int i = 0 ; i < Constants.STATISTICS_FILE; i++){

                        JavaSparkContext sc= Context.getContext("Query2");
                        JavaRDD<Tuple3<String,String,Double>>values = AllQueryPreProcess.executePreProcess(sc,Constants.FILE[i],2);

                        JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple2<Double,Double> > dt =  Query2Preprocess.executeProcess(sc,values,i);
                        //value = AllQueryPreProcess.executePreProcess(sc,Constants.FILE[i],2);
                        Query2_v2.executeQuery( dt, i );

                        sc.stop();
                        //Query2.executeQuery(sc, value);
                    }

                }

                break;

            case 3 :

                if (isSQL){
                    SQLQuery3.executeQuery(args);
                }else {

                    JavaSparkContext sc= Context.getContext("Query3");

                    JavaRDD<Tuple3<String,String,Double>> values = AllQueryPreProcess.executePreProcess(sc,Constants.TEMPERATURE_FILE,3);

                    JavaPairRDD<Tuple5<Integer, Integer,Integer,String, String>, Tuple2<Double,Double>> preprocess = Query3Preprocess.executeProcess(sc,values);

                    Query3_v2.executeQuery(preprocess);

                    sc.stop();


                   // Query3.executeQuery(args);
                }

                break;

            default:
                System.out.println("Exit");
                System.exit(0);
        }

    }
}
