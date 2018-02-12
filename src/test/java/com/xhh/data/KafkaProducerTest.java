package com.xhh.data;

import com.xhh.data.util.XHHConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerTest {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerTest.class);

    @Test
    public void producer() throws Exception {

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "172.19.19.231:9091,172.19.19.232:9091,172.19.19.233:9091");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(XHHConstants.KAFKA_LISTENER_TOPIC.getTopic(),
                0, "message", "okay");

        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);

        RecordMetadata recordMetadata = future.get();


        logger.info("recordMetadata: {}", recordMetadata.toString());

    }

}
