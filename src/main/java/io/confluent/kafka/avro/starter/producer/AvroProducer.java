package io.confluent.kafka.avro.starter.producer;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Properties;
import java.util.UUID;

@Slf4j
public class AvroProducer {
    private static final String TOPIC = "avro-topic";

    public static void main(String[] args) {
        // get schema from schema registry
        WebClient webClient = WebClient.builder()
                .baseUrl("http://localhost:8081")
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();

        WebClient.ResponseSpec responseSpec = webClient.get()
                .uri("/schemas/ids/1/schema")
                .retrieve();
        String responseBody = responseSpec.bodyToMono(String.class).block();
        //parse the schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(responseBody);

        AvroProducer avroProducer = new AvroProducer();
        avroProducer.writeMessage(schema);
    }

    public void writeMessage(Schema schema) {
        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        //prepare the avro record
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", UUID.randomUUID().toString());
        avroRecord.put("firstName", "John");
        avroRecord.put("lastName", "Doe");

        Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        log.info(String.valueOf(avroRecord));

        //prepare the kafka record
        ProducerRecord<String, GenericRecord> record
                = new ProducerRecord<>(TOPIC, "1234", avroRecord);

        producer.send(record);
        //ensures record is sent before closing the producer
        producer.flush();

        producer.close();
    }
}
