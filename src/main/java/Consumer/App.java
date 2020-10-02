package Consumer;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class App {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\winutils");

        StructType schema = new StructType()
                .add("search", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("userID", DataTypes.IntegerType)
                .add("current_ts", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("ecommerce-analysis")
                .config("spark.mongodb.output.uri", "mongodb://machine-ip/z-ecommerce.windows")
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");

        Dataset<Row> dataset = sparkSession.read().format("kafka")
                                                    .option("kafka.bootstrap.servers", "machine-ip:9092")
                                                    .option("subscribe", "search-analysis-stream").load();

        Dataset<Row> castedDataset = dataset.selectExpr("CAST(value AS STRING)");

        Dataset<Row> valueDataset = castedDataset.select(functions.from_json(castedDataset.col("value"), schema).as("data")).select("data.*");

        //Most Searched Products
        Dataset<Row> groupBySearchCount = valueDataset.groupBy("search").count().sort(functions.desc("count"));
        MongoSpark.write(groupBySearchCount).mode("append").option("collection","MostSearchedProducts").save();

        //Number of searches per user
        Dataset<Row> groupByUserID = valueDataset.groupBy(valueDataset.col("userID"), valueDataset.col("search")).count().filter("count>20");
        Dataset<Row> userSearch = groupByUserID.groupBy("userID").pivot("search").sum("count").na().fill(0);
        MongoSpark.write(userSearch).option("collection","searchByUserID").mode("append").save();

        //Number of searches per city
        Dataset<Row> cityGroupData = valueDataset.groupBy("city", "search").count().sort(functions.desc("count"));
        Dataset<Row> citySearchData = cityGroupData.groupBy("city").pivot("search").sum("count");
        MongoSpark.write(citySearchData).option("collection","searchByCity").mode("append").save();

        //Window - 1 Hour
        Dataset<Row> windowDataset = valueDataset.groupBy(functions.window(valueDataset.col("current_ts"), "60 minute"), valueDataset.col("search")).count().sort("window");
        MongoSpark.write(windowDataset).mode("append").save();
        windowDataset.groupBy("window").pivot("search").sum("count").sort("window").show();
    }


}
