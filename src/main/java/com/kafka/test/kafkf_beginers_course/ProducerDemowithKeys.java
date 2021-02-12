package com.kafka.test.kafkf_beginers_course;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemowithKeys {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final Logger logger = LoggerFactory.getLogger(ProducerDemowithKeys.class);

		String bootstrapServers = "127.0.0.1:9092";

		// create producer properties

		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {
			
			String topic = "first-topic";
			String value = "hello world"+Integer.toString(i);
			String key = "Id_"+ Integer.toString(i);

			// create a producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value);

			logger.info("Key: "+ key);
			// send data - asynchronous
			producer.send(record, new Callback() {
				// executes everytime when a record is successfully sent or thrown exception
				public void onCompletion(RecordMetadata metadata, Exception e) {

					if (e == null) {
						logger.info("Recieved new metadata. \n" + "Topic: " + metadata.topic() + "\n" + "Partition: "
								+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: "
								+ metadata.timestamp());
					} else {
						logger.error("Error while sending record to a topic :", e);
					}
				}
			}).get();
		}

		// flush data
		producer.flush();

		// flush and close producer
		producer.close();

		System.out.println("Hello World!");
	}
}
