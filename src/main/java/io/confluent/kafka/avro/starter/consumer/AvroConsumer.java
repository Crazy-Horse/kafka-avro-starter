package io.confluent.kafka.avro.starter.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
public class AvroConsumer {

    private static final String TOPIC = "avro-topic";

    public static void main(String[] args) {
        AvroConsumer avroConsumer = new AvroConsumer();
        avroConsumer.readMessages();
    }

    public void readMessages() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "protobuf-consumer-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
//        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,true);

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(TOPIC));

        //poll the record from the topic
        while (true) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, GenericRecord> record : records) {
                log.info("consumer first name: " + record.value().get("firstName"));
                log.info("consumer last name: " + record.value().get("lastName"));
            }
            consumer.commitAsync();
        }
    }
}
