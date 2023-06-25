package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class PartitionerProducer {

    private static final String TOPIC = "partitioner-topic-test";
    private static final String KEY = "partition_1";
    private static final String VALUE = "Zapisane na 1 partycji...";

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        sendMessageToKafkaTopic();
    }

    private static void sendMessageToKafkaTopic() throws InterruptedException, ExecutionException {
        // create producer
        try (Producer<String, String> producer = createProducer()) {

            // prepare record to send
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    TOPIC,
                    KEY,
                    VALUE
            );

            // send action
            System.out.println(producer.send(producerRecord).get());
        }
    }

    private static Producer<String, String> createProducer() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "out-producer");

        // Nasz partitioner
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OurPartitioner.class);

        return new KafkaProducer<>(properties);
    }

}
