package com.kafka;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MultiBrokerProducer {

	private static Producer<String, String> producer;
	private final Properties props = new Properties();
	public MultiBrokerProducer(){
		props.put("metadata.broker.list","localhost:9092,localhost:9096" );
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.kafka.SimplePartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String,String>(config);
	}
	//build message
	public static void main(String[] args) {
		MultiBrokerProducer prod = new MultiBrokerProducer();
		Random rnd = new Random();
		String topic =(String) args[0];
		
		for(int msgCount = 0;msgCount<10;msgCount++){
			Integer key = rnd.nextInt(225);
			String msg = "This message is for key: " + key + " message count:" + msgCount;
		Message kafkaMsg = new Message(msg.getBytes());
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, "2",msg);
			producer.send(data);
		}
		producer.close();
	}
}
