package com.xhh.data.kafka.listener;

import com.xhh.data.util.XHHConstants;
import com.xhh.data.websocket.service.WebsocketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ConsumerListener {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerListener.class);

    @Autowired
    private WebsocketService websocketService;

//    @KafkaListener(topics = XHHConstants.Constants.KAFKA_LISTENER_TOPIC)
    public void consumer(String message) {


        logger.info("consumer topic string get : {}", message);

        System.out.println(message);
//
//        Message messageReq = new Message();
//        messageReq.setMessage(message);
//
//        logger.info("send websocket request : {}", JsonHelper.toJson(messageReq).toString());
//
//        Response response = websocketService.send(messageReq);
//
//        logger.info("send websocket response : {}", JsonHelper.toJson(response).toString());

    }

}
