import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class App {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String INPUT_PATH = "src/main/resources/input.txt";

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java_word_count")
                .master("local[4]") // Replace 4 with the number of cores on your processor
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
                .getOrCreate();

        final String EH_SASL = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://scraper-dev.servicebus.windows.net/;SharedAccessKeyName=kafka-demo-mixed-policy;SharedAccessKey=/jB7QEZZsj2c8H8B1qWKE8NIrqDRb/Mysk7TASwQr7k=;EntityPath=kafka-demo\";";
        // final String EH_SASL = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://scraper-dev.servicebus.windows.net/;SharedAccessKeyName=kafka-demo-listen-policy;SharedAccessKey=U6SXIh9bPGWhVJmMMFbYcD8kPGkkcoeKPgR1f8csf4M=;EntityPath=kafka-demo";

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("subscribe", "kafka-demo")
                .option("kafka.bootstrap.servers", "scraper-dev.servicebus.windows.net:9093")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.jaas.config", EH_SASL)
                .option("kafka.request.timeout.ms", "60000")
                .option("kafka.session.timeout.ms", "30000")
                .option("kafka.group.id", "scraper-dev")
                .option("failOnDataLoss", "false")
                .load();

//        Dataset<Row> df = spark
//                .readStream()
//                .format("kafka")
//                .option("topic", "kafka-demo")
//                .option("kafka.bootstrap.servers", "scraper-dev.servicebus.windows.net:9093")
//                .option("kafka.sasl.mechanism", "PLAIN")
//                .option("kafka.security.protocol", "SASL_SSL")
//                .option("kafka.sasl.jaas.config", EH_SASL)
//                .option("kafka.batch.size", 5000)
//                .option("kafka.request.timeout.ms", 120000)
//                .option("kafka.session.timeout.ms", 60000)
//                .option("failOnDataLoss", "false")
//                .load();

        /*JavaRDD<String> lines = spark.read().textFile(INPUT_PATH).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        JavaPairRDD<Integer, String> swapped = counts.mapToPair(Tuple2::swap);
        swapped = swapped.sortByKey();

        List<Tuple2<String, Integer>> output = swapped.mapToPair(Tuple2::swap).collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }*/
        spark.stop();
    }
}
