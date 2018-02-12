package com.xhh.data;

import com.xhh.data.util.XHHConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class XHHApplication {
    private static final Logger logger = LoggerFactory.getLogger(XHHApplication.class);

    public static void main(String[] args) {

        SpringApplication.run(XHHApplication.class, args);

        Properties props = new Properties();
        props.put("bootstrap.servers", "172.19.19.231:9091,172.19.19.232:9091,172.19.19.233:9091");
        props.put("group.id", "mygroup");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(Arrays.asList(XHHConstants.KAFKA_LISTENER_TOPIC.getTopic()));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100 * 1000);
            for (ConsumerRecord<String, String> record : records) {
                String message = record.value();
                logger.info("consumer topic string get : {}", message);
                String sql = GreenplumSync.clearSql(message);
                logger.info("generate sql {}", sql);
                if (!StringUtils.isEmpty(sql)) {
                    try {
                        GreenplumSync.sync(sql);
                    } catch (Exception e) {
                        logger.error("sorry ,please check the sql " + sql);
                    }
                }
            }
        }
    }

}