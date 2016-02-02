
package it.unitn.bigdataprojet.kafkaspark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;

public final class Streaming {
	private static final Pattern XML = Pattern.compile("(?ms)<articolo>(?<abc>.*?)</articolo>");

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("Kafka-projectBigData");
		// Create the context with 13 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(13000));

		int numThreads = 2;
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put("bigdataproject", numThreads);

		JavaPairReceiverInputDStream<String, String> log = KafkaUtils.createStream(jssc, "localhost:2181",
				"kafka-spark-streaming", topicMap);

		JavaDStream<String> lines = log.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		JavaDStream<String> xmlList = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String x) {

				Matcher ee = XML.matcher(x);
				List<String> rr = new ArrayList<>();
				while (ee.find()) {
					rr.add(ee.group());
				}

				return Lists.newArrayList(rr);
			}
		});

		JavaPairDStream<String, Integer> xmlCount = xmlList.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		JavaDStream<String> keys = xmlCount.map(new Function<Tuple2<String, Integer>, String>() {
			@Override
			public String call(Tuple2<String, Integer> tuple) {
				return tuple._1();
			}

		});

		try {
			keys.dstream().saveAsTextFiles("hdfs://quickstart.cloudera:8020/user/cloudera/streaming/result/","dir");
		} catch (Exception e) {
			e.printStackTrace();
		}

		jssc.start();
		jssc.awaitTermination();
	}
}
