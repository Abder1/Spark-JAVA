import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ExempleSQLJAVA {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder()
                .master("local[3]")
                .appName("Java Spark SQL Example")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("src/main/resources/people.json");
        df.printSchema();
        df.show();
        df.select("name").show();
        df.createGlobalTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM global_temp.people");
        sqlDF.show();
        spark.stop();
    }
}
