package kafka.kafka.writer;

import java.io.Serializable;
import java.util.Properties;

import kafka.SparkJobListener;
import kafka.kafka.reader.KafkaConsumerPool;
import org.apache.spark.streaming.api.java.JavaDStream;

import scala.Tuple2;

public class JavaDStreamKafkaWriterFactory implements Serializable {

	private static final long serialVersionUID = -3309712415591664249L;

	public static JavaDStreamKafkaWriter getKafkaWriter(
			JavaDStream<Tuple2<String, String>> javaDStream, Properties config,String topic, Boolean kafkaAsync) {
		KafkaProducerPool kafkaProducerPool = new KafkaProducerPool(3, config);
		KafkaConsumerPool kafkaConsumerPool = new KafkaConsumerPool(3, config);
		javaDStream
				.context()
				.sparkContext()
				.addSparkListener(
						new SparkJobListener(kafkaConsumerPool,
								kafkaProducerPool,topic));
		
		return new JavaDStreamKafkaWriter(kafkaProducerPool, javaDStream, topic,kafkaAsync);
	}
}
