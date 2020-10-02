package Consumer;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class AppStream {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "C:\\winutils");

        StructType schema = new StructType()
                .add("search", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("userID", DataTypes.IntegerType)
                .add("current_ts", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("ecommerce-analysis-stream")
                .config("spark.mongodb.output.uri", "mongodb://machine-ip/z-ecommerce.windows")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "machine-ip:9092")
                .option("subscribe", "search-analysis-stream").load();
        final Dataset<Row> castedDataset = dataset.selectExpr("CAST(value AS STRING)");

       Dataset<Row> valueDataset = castedDataset.select(functions.from_json(castedDataset.col("value"), schema).as("data")).select("data.*");

        Dataset<Row> filter = valueDataset.filter(valueDataset.col("search").equalTo("deterjan"));

        filter.writeStream().foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                MongoSpark.write(rowDataset).option("collection","SearchStream").mode("append").save();
            }
        }).start().awaitTermination();

    }
}
