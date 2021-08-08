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

		HnApi hnApi = new HnApi();
		try {
			while (true) {
				for (int i = 19457630; i > 0; i--) {
					String hnData = hnApi.getById(i);
					if (i % 1000 == 0) {
						System.out.println(i);
					}
					String key = Long.toString(starterId++);
					ProducerRecord<String, String> data = new ProducerRecord<String, String>("hn_topic", key, hnData);
					System.out.println("sending topic");
					producer.send(data);
					long wait = Math.round(Math.random() * 25);
					Thread.sleep(wait);

					Thread.sleep(1000);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}
}
