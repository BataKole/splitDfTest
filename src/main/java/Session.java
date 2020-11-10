import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Session {

    private  SparkSession spark;


    public SparkSession getSpark(){
        return spark;
    }

    public Session() {
        spark = SparkSession
                .builder()
                .appName("java_spark_hive")
                .config("spark.driver.memory", "5g")
                .config("spark.executor.memory", "5g")
                .master("local[*]")
                .getOrCreate();
    }

    public Dataset<Row> loadDf(String path) {
        return spark.read()
                .json(path);
    }


}
