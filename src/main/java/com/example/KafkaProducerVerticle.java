package com.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class KafkaProducerVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerVerticle.class);

    private KafkaProducer<String, String> producer;

    @Override
    public void start() throws Exception {

        // Config values can be moved to application.properties
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");

        // use producer for interacting with Apache Kafka
        producer = KafkaProducer.create(vertx, config);

        // Sending a periodic message to kafka
        vertx.setPeriodic(2000, handler ->{

            // Creating the record
            KafkaProducerRecord<String, String> record = KafkaProducerRecord.create("test", "Hello sent at: "+ Instant.now());

            // Writing the record to Kafka and waiting for callback
            producer.write(record, done -> {

                if (done.succeeded()) {

                    RecordMetadata recordMetadata = done.result();
                    LOGGER.info("Message {} written on topic={}, partition={}, offset={}", record.value(), recordMetadata.getTopic(), recordMetadata.getPartition(), recordMetadata.getOffset());
                }

            });

        });
        super.start();
    }

    @Override
    public void stop() throws Exception {
        producer.close(res -> {
            if (res.succeeded()) {
                LOGGER.info("Producer is now closed");
            } else {
                LOGGER.warn("close failed");
            }
        });
        super.stop();
    }
}
