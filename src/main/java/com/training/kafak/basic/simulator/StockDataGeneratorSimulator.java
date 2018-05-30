package com.training.kafak.basic.simulator;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StockDataGeneratorSimulator {

	private static Logger logger = LoggerFactory
			.getLogger(StockDataGeneratorSimulator.class);

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException {
		Properties producerConfig = new Properties();
		producerConfig.put("bootstrap.servers", "127.0.0.1:9092");
		producerConfig.put("key.serializer",
				"org.apache.kafka.common" + ".serialization.StringSerializer");
		producerConfig.put("value.serializer",
				"org.apache.kafka.common" + ".serialization.IntegerSerializer");

		KafkaProducer<String, Integer> producer = new KafkaProducer(
				producerConfig);

		Random rng = new Random();

		String[] keys = {"COMP1", "COMP2", "COMP3"};
		Integer[] stockPrices = {100, 200, 300};

		int j = 0;
		while (j++ < 30) {

			logger.info("------ 5 second stock data -----");
			for (int i = 0; i < keys.length; i++) {
				Integer stockPrice = stockPrices[rng
						.nextInt(stockPrices.length)];
				String key = keys[i];
				ProducerRecord<String, Integer> record = new ProducerRecord<>(
						"stocks-data-1", key, stockPrice);
				producer.send(record);
				logger.info("key {} stock price {}", key, stockPrice);
			}
			// generating min wise stock price data
			Thread.sleep(30 * 1000);
		}
	}
}
