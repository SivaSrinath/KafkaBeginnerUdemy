package com.kafka.test.kafkf_beginers_course;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	public static void main(String[] args) {

		new ConsumerDemoWithThread().run();
	}
	
	private ConsumerDemoWithThread() {
		
	}
	
	public void run() {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-fifth-application";
		String topic = "first-topic";
		
		final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
		
		//Latch for dealing with multiple threads
		final CountDownLatch latch = new CountDownLatch(1);
		
		//Create the consumer runnable
		logger.info("Creating the consumer thread");
		final Runnable  myConsumerThread = new ConsumerThread(latch, bootstrapServers, groupId, topic); 
		
		//Start the thread
		Thread myThread = new Thread(myConsumerThread);
		myThread.start();
		
		//add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				logger.info("Caught a shutdown hook");
				((ConsumerThread)myConsumerThread).shutDown();
				try {
					latch.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				logger.info("Application has exited");
			}
		}
		));
		
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted", e);
		}finally {
			logger.info("Application is closing");
		}
		
	}

	class ConsumerThread implements Runnable {

		private CountDownLatch latch;

		// create consumer
		KafkaConsumer<String, String> consumer;
		final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

		private ConsumerThread(CountDownLatch latch, String bootstrapServers, String groupId, String topic) {

			this.latch = latch;

			// create consumer configs
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			consumer = new KafkaConsumer<String, String>(properties);

			// subscribe consumer to our topics
			consumer.subscribe(Arrays.asList(topic));
		}

		public void run() {

			try {
				// poll from new data
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

					for (ConsumerRecord<String, String> record : records) {
						logger.info("Key: " + record.key() + "," + "Value: " + record.value());
						logger.info(" Partition: " + record.partition() + "," + "Offset: " + record.offset());
					}
				}
			} catch (WakeupException e) {
				logger.info("Received shutdown signal");
			}finally {
				consumer.close();
				latch.countDown();
			}
		}

		public void shutDown() {
			// the wakeUp method is a special method to interrupt consumer.poll()
			consumer.wakeup();

		}

	}

}
