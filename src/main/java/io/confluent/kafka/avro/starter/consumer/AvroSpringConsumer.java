package io.confluent.kafka.avro.starter.consumer;

import io.confluent.kafka.avro.starter.model.MessageRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AvroSpringConsumer {

    private static final String TOPIC = "avro-topic";

    @KafkaListener(topics = TOPIC, groupId = "group_id_v1")
    public void processEvent(ConsumerRecord<String, GenericRecord> record) {
        MessageRequest messageRequest = new MessageRequest(String.valueOf(record.value().get("id")),
                String.valueOf(record.value().get("firstName")), String.valueOf(record.value().get("lastName")));

        // @Todo do some processing
        log.info(String.format("#### -> Consuming message -> %s", messageRequest));
    }

}
