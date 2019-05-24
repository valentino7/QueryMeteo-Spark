import Controllers.ControllerQuery1;
import Controllers.ControllerQuery2;
import Controllers.ControllerQuery3;
import Utils.Constants;

public class MainSpark {


    public static void main(String[] args) {

        if (args.length < 1 ){
            System.err.println(Constants.USAGE);
            System.exit(1);
        }

        String HDFS_ROOT = "hdfs://"+ args[0]+"/";

        //System.out.println(HDFS_ROOT);

        ControllerQuery1.executeMain(HDFS_ROOT, args[1]);
        ControllerQuery2.executeMain(HDFS_ROOT, args[1]);
        ControllerQuery3.executeMain(HDFS_ROOT, args[1]);
    }
}
