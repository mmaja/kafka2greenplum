package com.xhh.data;

import com.xhh.data.util.XHHConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by young on 2018/2/11.
 */
public class KafkaConsumerTest {
    public static void main(String[] args) {
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
            System.out.println("nothing available...");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100 * 1000);
            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                String sql = GreenplumSync.clearSql(record.value());

                try {
                    GreenplumSync.sync(sql);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println(sql);
            }
        }
    }
}

