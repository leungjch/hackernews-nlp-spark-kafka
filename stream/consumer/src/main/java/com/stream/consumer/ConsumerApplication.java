package com.stream.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;



import java.util.Arrays;
import java.util.Properties;

import java.util.logging.Logger;


@SpringBootApplication
public class ConsumerApplication {
	private static Logger LOGGERM = Logger.getLogger("InfoLogging");

	public static void main(String[] args) {

		LOGGERM.info("Logging an INFO-level message");

		System.out.println("HELLO");
		// Set up Java properties
		Properties props = new Properties();
		// This should point to at least one broker. Some communication
		// will occur to find the controller. Adding more brokers will
		// help in case of host failure or broker failure.
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "hn_consumer");
		// Enable a few useful properties for this example. Use of these
		// settings will depend on your particular use case.
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		// Required properties to process records
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		try {
			// List of topics to subscribe to
			consumer.subscribe(Arrays.asList("hn_topic"));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("**********************offset = %d, key = %s, value =%s%n", record.offset(),
							record.key(), record.value());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}

	}

}
