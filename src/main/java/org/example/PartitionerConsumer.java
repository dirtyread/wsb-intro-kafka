package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PartitionerConsumer {
    public static void main(String[] args) {
        subscribeOurMessages();
    }

    private static void subscribeOurMessages() {

        // consumer
        Consumer consumer = createConsumer();

        // subscribe
        consumer.subscribe(Collections.singletonList("partitioner-topic-test"));

        // messages records
        ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(10000));
        for (ConsumerRecord<String, String> consumerRecord : poll) {
            System.out.println(consumerRecord.value());
        }
    }

    private static Consumer createConsumer() {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id-conf-1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties);
    }
}
