package com.training.kafka.basic.ktable;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class MinuteWiseAvgStockPrice {

	public static void main(String[] args) throws InterruptedException {

		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG,
				"minute-wise-avg-stock-price");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				"localhost:9092");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());

		StreamsBuilder streamsBuilder = new StreamsBuilder();
		String sourceTopic = "stock-data-2";

		KStream<String, Integer> stockPriceStream = streamsBuilder
				.stream(sourceTopic);

		TimeWindows windows = TimeWindows.of(1000);
		Initializer<Integer> initializer = new Initializer<Integer>() {
			@Override
			public Integer apply() {
				return 0;
			}
		};
		Aggregator<String, Integer, Integer> aggregatorStockOccurance = new Aggregator<String, Integer, Integer>() {

			public Integer apply(String key, Integer value, Integer aggregate) {
				return aggregate + 1;
			}
		};

		Aggregator<String, Integer, Integer> aggregatorStockPrice = new Aggregator<String, Integer, Integer>() {

			public Integer apply(String key, Integer value, Integer aggregate) {
				return value + aggregate;
			}
		};
		KTable<Windowed<String>, Integer> stockMinWiseOccuranceCountTable = stockPriceStream
				.groupByKey().windowedBy(windows).aggregate(() -> 0,
						(aggKey, value, aggregate) -> aggregate + 1);

		KTable<Windowed<String>, Integer> stockMinWiseAggregateValue = stockPriceStream
				.groupByKey().windowedBy(windows)
				.reduce(new Reducer<Integer>() {

					@Override
					public Integer apply(Integer value1, Integer value2) {
						return value1 + value2;
					}
				});

		KTable<Windowed<String>, Integer> joinStockTables = stockMinWiseOccuranceCountTable
				.join(stockMinWiseAggregateValue,
						(occurance, total) -> total / occurance);

		KStream<String, Integer> sinkStream = joinStockTables
				.toStream((wk, v) -> wk.key());
		sinkStream.to("stock-min-avg-price");

		Topology topology = streamsBuilder.build();
		KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

		kafkaStreams.start();

	}
}
