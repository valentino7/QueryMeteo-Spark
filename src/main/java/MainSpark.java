import Controllers.ControllerQuery1;
import Controllers.ControllerQuery2;
import Controllers.ControllerQuery3;

public class MainSpark {


    public static void main(String[] args) {

        if (args.length < 1 ){
            System.err.println("Usage: MainSpark <HDFS_MasterIP:PORT>");
            System.exit(1);
        }

        String HDFS_ROOT = "hdfs://"+ args[0]+"/";

        ControllerQuery1.executeMain(HDFS_ROOT);
        ControllerQuery2.executeMain(HDFS_ROOT);
        ControllerQuery3.executeMain(HDFS_ROOT);
    }
}
