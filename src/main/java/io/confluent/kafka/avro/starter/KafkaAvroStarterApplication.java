package io.confluent.kafka.avro.starter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

@SpringBootApplication
public class KafkaAvroStarterApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaAvroStarterApplication.class, args);
    }
}
