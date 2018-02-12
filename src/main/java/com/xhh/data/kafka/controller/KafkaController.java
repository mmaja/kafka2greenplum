package com.xhh.data.kafka.controller;

import com.xhh.data.util.XHHConstants;
import com.xhh.data.websocket.vo.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaController(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/message/send")
    public String sendMessage(@RequestBody Message message) {
        kafkaTemplate.send(XHHConstants.KAFKA_LISTENER_TOPIC.getTopic(), message.getMessage());
        return message.getMessage();
    }


}
