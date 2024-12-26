package io.naveen.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallback {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("This is a kafka producer");

        //create producer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 50; i++) {

            String message = "hello===6557865=" + i;
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", message);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //executes everytime when a recod success or an exception
                    if(e == null){
                        log.info("Received new metadata \n" +
                                "Topic: "+ metadata.topic() + "\n" +
                                "partition: "+ metadata.partition()+ "\n" +
                                "offset: "+ metadata.offset()+ "\n" +
                                "timestamp: "+ metadata.timestamp());
                    } else {
                        log.error("error while sending the message", e);
                    }
                }
            });
        }


        //flush the producer
        producer.flush();

        producer.close();
    }
}
