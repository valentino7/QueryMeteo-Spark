import spark.Query1;
import spark.Query2;
import spark.Query3;
import sparkSQL.SQLQuery1;
import sparkSQL.SQLQuery2;
import sparkSQL.SQLQuery3;

public class Main {

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
                    Query1.executeQuery(args);
                }

                break;


            case 2:

                if (isSQL){
                    SQLQuery2.executeQuery(args);
                }else {
                    Query2.executeQuery(args);
                }

                break;

            case 3 :

                if (isSQL){
                    SQLQuery3.executeQuery(args);
                }else {
                    Query3.executeQuery(args);
                }

                break;

            default:
                System.out.println("Exit");
                System.exit(0);
        }

    }
}
