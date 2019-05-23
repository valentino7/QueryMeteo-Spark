package Utils;

public class Constants {

    public static final String MASTER ="local[*]";
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
    public static final String HDFS_MONGO_QUERY1 ="results/1";
    public static final String HDFS_MONGO_QUERY2 ="results/2/file";
    public static final String HDFS_MONGO_QUERY3 ="results/3";

    public static final String HDFS_MONGO_QUERY1_SQL ="results/4";
    public static final String HDFS_MONGO_QUERY2_SQL ="results/5/fileSQL";
    public static final String HDFS_MONGO_QUERY3_SQL ="results/6";

    public static final String HDFS_INPUT ="input/";


    public static final String USAGE = "Usage: MainSpark <HDFS_MasterIP:PORT> <mode>";
    public static final String QUERY1_NAME = "query1";
    public static final String QUERY2_NAME = "query2";
    public static final String QUERY3_NAME = "query3";
    public static final String HEADER_OPTION = "header";
    public static final String HEADER_BOOL = "true";
    public static final String JSON_FORMAT = "json";


    public static final String YEAR_LABEL = "year";
    public static final String MONTH_LABEL = "month";
    public static final String CITY_LABEL = "city";
    public static final String WEATHER_LABEL = "weather";
    public static final String COUNTRY_LABEL = "country";
    public static final String DAY_LABEL = "day";
    public static final String HOUR_LABEL = "hour";
    public static final String CURRENTYEAR_LABEL = "currentYear";
    public static final String LASTYEAR_LABEL = "lastYear";
    public static final String CURRENTPOSITION_LABEL = "currentPosition";
    public static final String LASTPOSITION_LABEL = "lastPosition";
    public static final String TEMPERATURE_LABEL = "temperature";

    public static final String VALUE_LABEL = "value";
    public static final String COUNT_LABEL = "count";


    public static final String GEONAMES_USERNAME = "anthony2801";
    public static final String SPAK_SUCCESS_CONF = "mapreduce.fileoutputcommitter.marksuccessfuljobs";
    public static final String SPAK_SUCCESS_BOOL = "false";

    public static final String WEATHER ="sky is clear" ;
    public static final String AVERAGE_LABEL = "avg";
    public static final String MIN_LABEL = "min";
    public static final String MAX_LABEL = "max";
    public static final String STDDEV_LABEL = "std dev";

    public static final String DATEPATTERN = "yyyy-MM-dd HH:mm:ss";


}
