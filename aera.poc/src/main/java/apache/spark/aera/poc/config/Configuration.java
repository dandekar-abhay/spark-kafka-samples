package apache.spark.aera.poc.config;

import java.util.HashMap;
import java.util.Map;

public class Configuration {

	public static final String KAFKA_ZK_QUORUM = "fusionops-1:2181";
	
	/**
	 * Topic name to be referred to
	 */
	public static final String KAFKA_TOPIC = "aeroTopic7";
	
	/**
	 * Kafka broker details along with port
	 */
	public static final String KAFKA_BROKER = "fusionops-2:9092";
	
	/**
	 * Group ID to be used for kafka
	 */
	public static final String KAFKA_GROUP_ID = "";
	
	
	public static final Map<String, String> KAFKA_PROPERTIES = new HashMap<String, String>();
	
	static {
		// Add all the kafka properties here
		KAFKA_PROPERTIES.put("batch.size", "16384");
		KAFKA_PROPERTIES.put("acks", "all");
		KAFKA_PROPERTIES.put("linger.ms", "1");
		KAFKA_PROPERTIES.put("buffer.memory", "16000");
	}
	
}
