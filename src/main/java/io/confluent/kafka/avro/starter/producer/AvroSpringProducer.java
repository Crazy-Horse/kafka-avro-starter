package io.confluent.kafka.avro.starter.producer;

import io.confluent.kafka.avro.starter.model.MessageRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.UUID;


@Service
@Slf4j
public class AvroSpringProducer {

    private static final String TOPIC = "avro-topic";

    @Autowired
    private KafkaTemplate<String, GenericRecord> kafkaTemplate;

    public void sendMessage(MessageRequest message) {
        log.info(String.format("#### -> Producing message -> %s", message));

        //prepare the avro record
        GenericRecord avroRecord = new GenericData.Record(getSchema());
        avroRecord.put("id", message.getId());
        avroRecord.put("firstName", message.getFirstName());
        avroRecord.put("lastName", message.getLastName());

        this.kafkaTemplate.send(TOPIC, message.getId(), avroRecord);
    }

    private Schema getSchema() {
        // can optimize by caching schema
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
        return parser.parse(responseBody);
    }
}
