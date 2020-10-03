package com.hazelcast.jet.demos.traintrack;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.topic.ITopic;

/**
 * <p>Some plumbing. Attach a listener to a specific Hazelcast
 * topic that feeds messages to a web socket.
 * </p>
 */
@Configuration
public class ApplicationRunner implements CommandLineRunner {

    private static final String TOPIC_NAME = "treno";

    @Autowired
    private HazelcastInstance hazelcastInstance;
    @Autowired
    private MySocketSendListener mySocketSendListener;

    /**
     * <p>Note the topic name ("{@code treno}") needs to match between the sender
     * (Hazelcast server) and registered clients. So it should really be a constant
     * field.
     * </p>
     */
    @Override
    public void run(String... args) throws Exception {
        ITopic<HazelcastJsonValue> iTopic =
                this.hazelcastInstance.getTopic(TOPIC_NAME);

        iTopic.addMessageListener(this.mySocketSendListener);
    }

}
