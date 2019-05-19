package Utils;

public class Constants {

    public static final String MASTER ="local";
    public static final int STATISTICS_FILE = 3;
    public static final String[] FILE = {"temperature.csv","pressure.csv","humidity.csv"};
    public static final String CITY_FILE_PARQUET ="city_attributes.parquet";
    public static final String CITY_FILE_CSV ="city_attributes.csv";
    public static final String WEATHER_FILE_CSV ="weather_description.csv";
    public static final String WEATHER_FILE_PARQUET ="weather_description.parquet";

    public static final String PRESSURE_FILE_CSV ="pressure.csv";
    public static final String TEMPERATURE_FILE_CSV ="temperature.csv";
    public static final String TEMPERATURE_FILE_PARQUET ="temperature.csv";
    public static final String HUMIDITY_FILE ="humidity.csv";
    public static final int FILE_TYPE =0;
    public static final int NUM_OF_FILE =2;

    public static final String HDFS_ROOT="hdfs://172.18.0.5:54310/";
    public static final String HDFS_HBASE_QUERY1 =HDFS_ROOT+"results/hbase/query1";
    public static final String HDFS_HBASE_QUERY2 =HDFS_ROOT+"results/hbase/query2/file";
    public static final String HDFS_HBASE_QUERY3 =HDFS_ROOT+"results/hbase/query3";

    public static final String HDFS_MONGO_QUERY1 =HDFS_ROOT+"results/mongo/query1";
    public static final String HDFS_MONGO_QUERY2 =HDFS_ROOT+"results/mongo/query2/file";
    public static final String HDFS_MONGO_QUERY3 =HDFS_ROOT+"results/mongo/query3";

    public static final String HDFS_MONGO_QUERY1_SQL =HDFS_ROOT+"results/mongoSQL/query1";
    public static final String HDFS_MONGO_QUERY2_SQL =HDFS_ROOT+"results/mongoSQL/query2/file";
    public static final String HDFS_MONGO_QUERY3_SQL =HDFS_ROOT+"results/mongoSQL/query3";

    public static final String HDFS_INPUT =HDFS_ROOT+"input/";


}
