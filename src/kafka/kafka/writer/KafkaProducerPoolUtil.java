package kafka.kafka.writer;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Tuple2;


/**
 *
 * @author Oleg Varaksin, Farouk Salem
 *
 */
public class KafkaProducerPoolUtil implements Serializable {

    private static final long serialVersionUID = -1913028296093224674L;

    private transient ConcurrentLinkedQueue<KafkaProducer<String, String>> pool;

    private ScheduledExecutorService executorService;

    private final Properties properties;

    private final int minIdle;

    private static KafkaProducerPoolUtil instance = null;
    /**
     * Single Instance
     *
     * */



    public static KafkaProducerPoolUtil getInstance(final int minIdle, final Properties properties){
        if(instance == null){
            instance  = new KafkaProducerPoolUtil(minIdle, properties);
            return instance;
        }
        else
            return instance;
    }

    public static KafkaProducerPoolUtil getInstance(final int minIdle, final int maxIdle,
                                                    final long validationInterval, final Properties properties){
        if(instance == null){
            instance  = new KafkaProducerPoolUtil(minIdle,maxIdle,validationInterval, properties);
            return instance;
        }
        else
            return instance;
    }


    /**
     * Creates the pool.
     *
     * @param minIdle
     *            minimum number of objects residing in the pool
     */
    private KafkaProducerPoolUtil(final int minIdle, final Properties properties) {

        System.out.println("creating Kafka Pool");
        // initialize pool
        this.properties = properties;
        this.minIdle = minIdle;
        initialize();

    }

    /**
     * Creates the pool.
     *
     * @param minIdle
     *            minimum number of objects residing in the pool
     * @param maxIdle
     *            maximum number of objects residing in the pool
     * @param validationInterval
     *            time in seconds for periodical checking of minIdle / maxIdle
     *            conditions in a separate thread. When the number of objects is
     *            less than minIdle, missing instances will be created. When the
     *            number of objects is greater than maxIdle, too many instances
     *            will be removed.
     */
    private KafkaProducerPoolUtil(final int minIdle, final int maxIdle,
                                  final long validationInterval, final Properties properties) {
        // initialize pool
        this.properties = properties;
        this.minIdle = minIdle;
        initialize();

        // check pool conditions in a separate thread
        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                int size = pool.size();
                if (size < minIdle) {
                    int sizeToBeAdded = minIdle - size;
                    for (int i = 0; i < sizeToBeAdded; i++) {
                        pool.add(createProducer());
                    }
                } else if (size > maxIdle) {
                    int sizeToBeRemoved = size - maxIdle;
                    for (int i = 0; i < sizeToBeRemoved; i++) {
                        pool.poll();
                    }
                }
            }
        }, validationInterval, validationInterval, TimeUnit.SECONDS);
    }

    /**
     * Gets the next free object from the pool. If the pool doesn't contain any
     * objects, a new object will be created and given to the caller of this
     * method back.
     *
     * @return T borrowed object
     */
    public synchronized KafkaProducer<String, String> borrowProducer() {
        if (pool == null)
            initialize();
        KafkaProducer<String, String> object;
        if ((object = pool.poll()) == null) {
            object = createProducer();
        }

        return object;
    }

    /**
     * Returns object back to the pool.
     *
     *            object to be returned
     */
    public void returnProducer(KafkaProducer<String, String> producer) {
        if (producer == null) {
            return;
        }
        this.pool.offer(producer);
    }

    /**
     * Shutdown this pool.
     */
    public void shutdown() {
        if (executorService != null) {
            KafkaProducer<String, String> producer;
            while ((producer = pool.poll()) != null) {
                producer.close();
            }
            executorService.shutdown();
        }
    }

    /**
     * Creates a new producer.
     *
     * @return T new object
     */
    private KafkaProducer<String, String> createProducer() {
        System.out.println("creating producer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }

    private void initialize() {
        pool = new ConcurrentLinkedQueue<KafkaProducer<String, String>>();

        for (int i = 0; i < minIdle; i++) {
            pool.add(createProducer());
        }
    }

    public void closeAll() {
        KafkaProducer<String, String> object;
        while ((object = pool.poll()) != null) {
            //object.flush();
            object.close();
        }
    }

    public static void writeToKafka(String topic, Tuple2<String, String> message , boolean async  , KafkaProducer<String, String>  producer) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                topic, message._1(), message._2());
        if (async) {
            producer.send(record);
        } else {
            try {
                producer.send(record).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
