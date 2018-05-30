package com.training.kafka.basic.ktable;

import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinuteWiseAvgStockPrice {

	private static Logger logger = LoggerFactory
			.getLogger(MinuteWiseAvgStockPrice.class);

	public static void main(String[] args) throws InterruptedException {

		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG,
				"minute-wise-avg-stock-price");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				"localhost:9092");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
				Serdes.Integer().getClass().getName());
		// cache.max.bytes.buffering=0
		properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		StreamsBuilder streamsBuilder = new StreamsBuilder();
		String sourceTopic = "stocks-data-1";

		KStream<String, Integer> stockPriceStream = streamsBuilder
				.stream(sourceTopic);

		// Min wise stock price avg aggregation
		TimeWindows windows = TimeWindows.of(60 * 1000);

		Aggregator<String, Integer, Integer> occuranceAggregation = new Aggregator<String, Integer, Integer>() {

			@Override
			public Integer apply(String key, Integer value, Integer aggregate) {
				logger.info("key {} total occurance: {}", key, (aggregate + 1));
				return aggregate + 1;
			}
		};
		KTable<Windowed<String>, Integer> stockMinWiseOccuranceCountTable = stockPriceStream
				.groupByKey().windowedBy(windows)
				.aggregate(() -> 0, occuranceAggregation);

		Reducer<Integer> totalSumReducer = new Reducer<Integer>() {

			@Override
			public Integer apply(Integer value1, Integer value2) {
				logger.info("total sum: {} ", (value1 + value2));
				return value1 + value2;
			}
		};

		KTable<Windowed<String>, Integer> stockMinWiseAggregateValue = stockPriceStream
				.groupByKey().windowedBy(windows).reduce(totalSumReducer);

		ValueJoiner<Integer, Integer, Integer> avgJoiner = new ValueJoiner<Integer, Integer, Integer>() {

			@Override
			public Integer apply(Integer value1, Integer value2) {
				logger.info("avg {}", value1 / value2);
				return value1 / value2;
			}
		};
		KTable<Windowed<String>, Integer> joinStockTables = stockMinWiseAggregateValue
				.join(stockMinWiseOccuranceCountTable, avgJoiner);
		KeyValueMapper<Windowed<String>, Integer, String> stockMinuteWiseData = new KeyValueMapper<Windowed<String>, Integer, String>() {

			@Override
			public String apply(Windowed<String> key, Integer value) {
				logger.info("Writing to output stream ==> " + new Date()
						+ " window start {} window end {} key {} value {}",
						key.window().start(), key.window().end(), key.key(),
						value);
				return key.key();
			}
		};
		// (wk, v) -> wk.key()
		KStream<String, Integer> sinkStream = joinStockTables
				.toStream(stockMinuteWiseData);

		sinkStream.to("stock-min-avg-price");

		Topology topology = streamsBuilder.build();
		KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
		kafkaStreams.start();
		logger.info("Stream is started");
	}
}

class LogAndContinueExceptionHandler
		implements
			DeserializationExceptionHandler {
	private static final Logger log = LoggerFactory
			.getLogger(LogAndContinueExceptionHandler.class);

	@Override
	public DeserializationHandlerResponse handle(final ProcessorContext context,
			final ConsumerRecord<byte[], byte[]> record,
			final Exception exception) {

		log.warn(
				"Exception caught during Deserialization, "
						+ "taskId: {}, topic: {}, partition: {}, offset: {}",
				context.taskId(), record.topic(), record.partition(),
				record.offset(), exception);

		return DeserializationHandlerResponse.CONTINUE;
	}

	@Override
	public void configure(final Map<String, ?> configs) {
		// ignore
	}
}