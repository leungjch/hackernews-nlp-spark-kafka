package com.stream.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.stream.producer.api.HnApi;

@SpringBootApplication
public class ProducerApplication {
	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);

		long events = 1000;
		long starterId = Math.round(Math.random() * Integer.MAX_VALUE);

		// Set up Java properties
		Properties props = new Properties();
		// This should point to at least one broker. Some communication
		// will occur to find the controller. Adding more brokers will
		// help in case of host failure or broker failure.
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// Enable a few useful properties for this example. Use of these
		// settings will depend on your particular use case.
		props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
		// Required properties to process records
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		// Set to null initially
		int currentId = -1;
		HnApi hnApi = new HnApi();
		try {
			while (true) {

				int latestId = hnApi.getLatestId();
				// If new id
				if (currentId != latestId) {
					if (currentId == -1) {
						currentId = latestId;
						continue;
					} else {

						System.out.println(latestId);
						Thread.sleep(2000); // Delay before we send the request to prevent requesting null data

						// Assumption, latestId > currentId
						// Fetch the new data
						for (int i = currentId + 1; i <= latestId; i++) {
							String hnData = hnApi.getById(i);

							String key = Long.toString(i);
							ProducerRecord<String, String> data = new ProducerRecord<String, String>("hn_topic", key,
									hnData);
							System.out.println("sending topic for index " + i);
							System.out.println(hnData);

							producer.send(data);

							Thread.sleep(1000);

						}
						currentId = latestId;

					}

				}

				// Update every 15 seconds
				Thread.sleep(15000);

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}
}
