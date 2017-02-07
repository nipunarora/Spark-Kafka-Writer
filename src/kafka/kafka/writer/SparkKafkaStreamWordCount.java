package kafka.kafka.writer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class SparkKafkaStreamWordCount implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// static Logger logger = Logger.getLogger(SparkWordCount.class);
	// static {
	// logger.setLevel(Level.ERROR);
	// }
	// private final String LOCAL_SPARK = "spark://ws-37:7077";
	// private final String SERVER_SPARK = "spark://stream-demo.novalocal:7077";
	// private final String SERVER_SPARK = "spark://192.168.74.30:7077";

	public static JavaDStream<String> getKafkaDStream(String inputTopics, String broker, int kafkaPort, JavaStreamingContext ssc){
		HashSet<String> inputTopicsSet = new HashSet<String>(Arrays.asList(inputTopics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", broker + ":" + kafkaPort);

		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
				ssc,
				String.class,
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParams,
				inputTopicsSet
		);

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		return lines;
	}

	public void run(Boolean groupByKey) {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local").setAppName("test");

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(10*1000));

		ssc.sparkContext().setLogLevel("OFF");

		JavaDStream<String> lines = getKafkaDStream("inputTopic", "arya.nec-labs.com", 9092, ssc);

		lines.print(2);

		JavaPairDStream<String, Integer> pair = lines.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String,Integer>(s,1);
			}
		});

		JavaPairDStream<String, Integer> output = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer + integer2;
			}
		});

		JavaDStream<Tuple2<String,String>> wordCount = output.map(new Function<Tuple2<String, Integer>, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
				return new Tuple2<String,String>(stringIntegerTuple2._1(),stringIntegerTuple2._2().toString());
			}
		});


		Properties props = new Properties();
		props.put("bootstrap.servers", "arya.nec-labs.com:2181");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("block.on.buffer.full", "true");
		props.put("auto.offset.reset", "earliest");
		props.put("group.id", "test-group");
		props.put("auto.commit.enable", "false");

		wordCount.print(2);

		JavaDStreamKafkaWriter writer = JavaDStreamKafkaWriterFactory.getKafkaWriter(wordCount, props, "oututTopic", true);
		writer.writeToKafka();

		ssc.start();
		ssc.awaitTermination();
	}


	public static void main(String[] args) {
		System.out.println("program started at " + System.currentTimeMillis());
		new SparkKafkaStreamWordCount().run(false);
	}
}
