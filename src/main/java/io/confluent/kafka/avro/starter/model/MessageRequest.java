package io.confluent.kafka.avro.starter.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageRequest {
    private String id;
    private String firstName;
    private String lastName;
}
