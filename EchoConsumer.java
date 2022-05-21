import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord; 

public class EchoConsumer {
    public final static int KB     = 1024;
    public final static int MB     = 1024 * KB;

	public static void main(String[] args) throws Exception {
		String topicName = "hello";
		//Kafka consumer configuration settings
		Properties consumer_props = new Properties();
		consumer_props.put("bootstrap.servers", "localhost:9092");
		consumer_props.put("group.id", "echo");
		consumer_props.put("enable.auto.commit", "true");
		consumer_props.put("auto.commit.interval.ms", "1000");
		consumer_props.put("session.timeout.ms", "30000");
		consumer_props.put("key.deserializer", 
				"org.apache.kafka.common.serializa-tion.StringDeserializer");
		consumer_props.put("value.deserializer", 
				"org.apache.kafka.common.serializa-tion.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer
			<String, String>(consumer_props, new StringDeserializer(), new StringDeserializer());
		consumer.subscribe(Arrays.asList(topicName));

		Properties producer_props = new Properties();
		producer_props.put("bootstrap.servers", "localhost:9092");
		producer_props.put("acks", "all");
		producer_props.put("retries", 0);
		producer_props.put("batch.size", 4);
		producer_props.put("linger.ms", 1);
		producer_props.put("buffer.memory", 33554432);
		producer_props.put("key.serializer", 
				"org.apache.kafka.common.serializa-tion.StringSerializer");
		producer_props.put("value.serializer", 
				"org.apache.kafka.common.serializa-tion.StringSerializer");
		Producer<String, String> producer = new KafkaProducer
			<String, String>(producer_props, new StringSerializer(), new StringSerializer());

		//print the topic name
		System.out.println("Subscribed to topic " + topicName);
		int i = 0;
        char[][] chars = new char[10][KB];
        for (i = 0; i < 10; i++)
           Arrays.fill(chars[i], (char)(i+48));

        boolean receive_flag = false;

        int key = 0;
        String[] message =new String[10];
        /*
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records){
                key = Integer.parseInt(record.key());
                message[key] = record.value();
                if(key < 10){
                    if (key == 9) {
                        receive_flag = true;
                        break;
                    }
                }
            }
            if (receive_flag) break;
        }
        for(key = 10; key < 20; key++){
            producer.send(new ProducerRecord<String, String>(topicName, 
                        Integer.toString(key), message[key-10]));
        }
        consumer.close();
        producer.close();

        System.out.println("Done echoing.");
        */
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records){
                key = Integer.parseInt(record.key());
                String value = record.value();
                if(value.charAt(0) == '$'){
                    //End flag, we stop echoing.
                    receive_flag = true;
                    break;
                }
                if (key < 10000) {
                    // Normal testing message, we echo back with higher keys.
                    producer.send(new ProducerRecord<String, String>(topicName, 
                                Integer.toString(key+10000), value));
                    //System.out.printf("Echo Key %d with %d\n.", key, key+10000);
                }
            }
            producer.flush();
            if (receive_flag) break;
        }
        consumer.close();
        producer.close();
        System.out.println("Done echoing.");
    }
}
