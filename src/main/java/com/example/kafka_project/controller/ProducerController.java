package com.example.kafka_project.controller;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/producer")
public class ProducerController {
    private static final String TOPIC_NAME = "tasty";
    private static final String BOOTSTRAP_SERVERS = "192.168.56.101:9092";
    private final Producer<String, String> kafkaProducer;

    public ProducerController() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<>(props);
    }

    @PostMapping("/event")
    public void sendEventToKafka(@RequestBody String eventData) {
        // here user Event is the key which represents some partition
        ProducerRecord record = new ProducerRecord<>(TOPIC_NAME, "userEvent", eventData);
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.out.println("Error sending data to kafka" + exception.getMessage());
            } else {
                System.out.println("Event sended to kafka offset : " + metadata.offset());
            }
        });

    }

}
