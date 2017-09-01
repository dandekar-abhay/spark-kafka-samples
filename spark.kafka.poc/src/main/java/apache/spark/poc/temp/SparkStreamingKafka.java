package apache.spark.poc.temp;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import apache.spark.poc.config.Configuration;

import java.util.Arrays;

public class SparkStreamingKafka {
	
	public static void main(String[] args) throws StreamingQueryException {
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("JavaStructuredKafkaWordCount")
				  .master("spark://fusionops-1:7077")
				  .getOrCreate();
		
		System.out.println("Spark session created successfully");
		
	    // Create DataSet representing the stream of input lines from kafka
	    Dataset<String> lines = spark
	      .readStream()
	      .format("kafka")
	      .option("kafka.bootstrap.servers", Configuration.KAFKA_BROKER)
	      .option("subscribe", Configuration.KAFKA_TOPIC)
	      .load()
	      .selectExpr("CAST(value AS STRING)")
	      .as(Encoders.STRING());
		
	    // Generate running word count
	    Dataset<Row> wordCounts = lines.flatMap(
	        (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
	        Encoders.STRING()).groupBy("value").count();
		
	    // Start running the query that prints the running counts to the console
	    StreamingQuery query = wordCounts.writeStream()
	      .outputMode("complete")
	      .format("console")
	      .start();
	        
	    query.awaitTermination();
		
	}

}
