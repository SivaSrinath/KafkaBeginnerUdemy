package com.kafka.test.kafkf_beginers_course;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		
		Properties properties = new Properties();
		
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-fourth-application";
		String topic = "first-topic";
		
		//create consumer configs
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,  groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		//subscribe consumer to our topics
		consumer.subscribe(Arrays.asList(topic));
		
		//poll from new data
		while(true) {
			ConsumerRecords<String, String> records = 
					consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record :records) {
				logger.info("Key: "+record.key() + ","+ "Value: "+ record.value());
				logger.info(" Partition: "+ record.partition() + ","+"Offset: "+ record.offset());
			}
		}

	}

}
