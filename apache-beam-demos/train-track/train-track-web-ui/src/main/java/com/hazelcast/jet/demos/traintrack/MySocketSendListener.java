package com.hazelcast.jet.demos.traintrack;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>This code is just a connector.
 * </p>
 * <p>Everything received on a specific Hazelcast {@code com.hazelcast.core.ITopic Topic}
 * is pushed out to a web socket. One in, one out.
 * </p>
 */
@Component
@Slf4j
public class MySocketSendListener implements MessageListener<HazelcastJsonValue> {

     private static final String DESTINATION =
             "/" + ApplicationConstants.QUEUE_NAME
             + "/" + ApplicationConstants.QUEUE_NAME_LOCATION;

     static {
         log.info("Destination: {}", DESTINATION);
     }

     @Autowired
     private SimpMessagingTemplate simpMessagingTemplate;

    /**
     * <p>Republish from Hazelcast's {@code com.hazelcast.core.ITopic Topic}
     * to the web socket.
     * </p>
     */
    @Override
    public void onMessage(Message<HazelcastJsonValue> message) {

        HazelcastJsonValue payload = message.getMessageObject();
        log.info("{}", payload);

        this.simpMessagingTemplate.convertAndSend(DESTINATION, payload.toString());
    }

}
