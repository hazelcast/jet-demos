package com.hazelcast.jet.demos.traintrack;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * <p>Create a message broker so this application
 * can communicate with a web socket.
 * </p>
 */
@Configuration
@EnableWebSocketMessageBroker
public class MyWebSocketMessageBrokerConfigurer implements WebSocketMessageBrokerConfigurer {

    /**
     * <p>Add the standard message broker for a specific destination
     * prefix.
     * </p>
     */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry messageBrokerRegistry) {
        messageBrokerRegistry
        .enableSimpleBroker("/" + ApplicationConstants.QUEUE_NAME);
    }

    /**
     * <p>Register and endpoint for the
     * <a href="https://stomp.github.io/">stomp</a> protocol,
     * using the <a href="https://github.com/sockjs">socks.js</a>
     * Javascript library.
     * </p>
     */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry stompEndpointRegistry) {
        stompEndpointRegistry
        .addEndpoint("/" + ApplicationConstants.WEBSOCKET_ENDPOINT)
        .setAllowedOrigins("*")
        .withSockJS();
    }

}
