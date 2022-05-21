//import util.properties packages
import java.util.Properties;
import java.util.Arrays;
import java.lang.Math;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord; 
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer; 

public class SimpleProducer {
    public final static int KB     = 1024;
    public final static int MB     = 1024 * KB;
    public final static char end_flag     = '$';
    public final static String topicName = "hello";
    /*
    KafkaConsumerRunner is made an inner class because this code will be transported to Mendix, where
    it is difficult to write another outer class.
    */
    public class KafkaConsumerRunner implements Runnable {
        public KafkaConsumer<String, String> consumer;
        public Thread t;
        public int end_key;

        public KafkaConsumerRunner(int ek) {
            end_key = ek;
            // Init consumer.
            Properties consumer_props = new Properties();
            consumer_props.put("bootstrap.servers", "localhost:9092");
            consumer_props.put("group.id", "producer_consumer");
            consumer_props.put("enable.auto.commit", "true");
            consumer_props.put("auto.commit.interval.ms", "1000");
            consumer_props.put("session.timeout.ms", "30000");
            consumer_props.put("key.deserializer", 
                    "org.apache.kafka.common.serializa-tion.StringDeserializer");
            consumer_props.put("value.deserializer", 
                    "org.apache.kafka.common.serializa-tion.StringDeserializer");
            this.consumer = new KafkaConsumer
                <String, String>(consumer_props, new StringDeserializer(), new StringDeserializer());
            consumer.subscribe(Arrays.asList(topicName));
        }

        @Override
        public void run() {
            System.out.println("Inside run.");
            try {
                consumer.subscribe(Arrays.asList(topicName));
                boolean receive_flag = false;
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    //System.out.println("Making a new poll.");
                    // Handle new records
                    for (ConsumerRecord<String, String> record : records){
                        //System.out.println("Producer Finds New Key " + record.key());
                        if (Integer.parseInt(record.key()) == this.end_key) {
                            receive_flag = true;
                            break;
                        }
                    }
                    if(receive_flag){
                        //System.out.println("All the message are received.");
                        break;
                    }
                }
            } catch (Exception e) {
                // Ignore exception if closing
                System.out.println(e);
            } finally {
                consumer.close();
                //System.out.println("Reader for the consumer closed.");
            }
        }

        public void start(){
            System.out.printf("Start Kafka Consumer Runner, waiting for key %d.\n", end_key);
            if (t == null) {
                t = new Thread (this, "Kafka Consumer");
                t.start();
            }
        }
    }

    public static void main(String[] args) throws Exception{
        // create instance for properties to access producer configs   
        Properties producer_props = new Properties();
        //Assign localhost id
        producer_props.put("bootstrap.servers", "localhost:9092");
        //Set acknowledgements for producer requests.      
        producer_props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        producer_props.put("retries", 0);
        //Specify buffer size in config. We set the batch size to 10 to make sure that the system can perform
        // the best in our performance experiments.
        producer_props.put("batch.size", 10);
        //Reduce the no of requests less than 0   
        producer_props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.   
        producer_props.put("buffer.memory", 33554432);
        producer_props.put("key.serializer", 
                "org.apache.kafka.common.serializa-tion.StringSerializer");
        producer_props.put("value.serializer", 
                "org.apache.kafka.common.serializa-tion.StringSerializer");
        Producer<String, String> producer = new KafkaProducer
            <String, String>(producer_props, new StringSerializer(), new StringSerializer());
        //int[] msg_size = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8*KB, 16*KB, 32*KB, 64*KB, 128*KB, 256*KB, MB/2};
        int[] msg_size = {1*KB, 100*KB, 200*KB, 300*KB, 400*KB, 500*KB, 600*KB, 700*KB, 800*KB, 900*KB, 1000*KB};
        long[] duration = new long[msg_size.length];
        for (int i = 0; i < msg_size.length; i++) {
            duration[i] = new SimpleProducer().performance_test(producer, i*100, 100, msg_size[i]);
        }
        for (int i = 0; i < msg_size.length; i++){
            System.out.println(duration[i]/100/1000);
        }
        //Notify the echo receiver to stop.
        producer.send(new ProducerRecord<String, String>(topicName, 
                    Integer.toString(9999), "$"));
        producer.close();
    }

    public long performance_test(Producer<String, String> producer, int start_key, int message_count,
                                 int message_size) {
        System.out.printf("Performance test for StartKey: %d, MessageSize: %d.\n", start_key, message_size);
        KafkaConsumerRunner kcr = new SimpleProducer().new KafkaConsumerRunner(start_key+message_count+10000-1);
        kcr.start();
        try {
            Thread.sleep(1000);
         } catch (Exception e) {
            System.out.println("Produceer cannot sleep.");
         }
        System.out.println("Wake up in the producer.");

        char[][] chars = new char[message_count][message_size];
        for (int i = 0; i < message_count; i++)
            Arrays.fill(chars[i], (char)(i%10+48));

        boolean receive_flag = false;
        long startTime = System.nanoTime();

        for(int i = 0; i < message_count; i++)
            producer.send(new ProducerRecord<String, String>(topicName, 
                        Integer.toString(start_key+i), String.valueOf(chars[i])));
        producer.flush();
        // Wait for the consumer thread to join to make sure we get all the echos back.
        //System.out.println("All message are sent, waiting for response.");
        try {
            kcr.t.join();
        } catch (Exception e) {
            System.out.println("Cannot join the thread.");
        }

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
        System.out.printf("Total time consumption is %d.\n", duration);
        return duration;
    }
}
