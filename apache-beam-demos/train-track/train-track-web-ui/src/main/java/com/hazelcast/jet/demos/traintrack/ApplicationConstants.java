package com.hazelcast.jet.demos.traintrack;

/**
 * <p>Define some constants to use in the application.
 * Queue stuff is the web socket sending. The end point is
 * how the web application will connect.
 * </p>
 */
public class ApplicationConstants {

    // This pair needs to match "stompClient.subscribe('/queue/location'" in app.js file
    public static final String QUEUE_NAME = "queue";
    public static final String QUEUE_NAME_LOCATION = "location";

    // Needs to match "new SockJS('/hazelcast')" in app.js file
    public static final String WEBSOCKET_ENDPOINT = "hazelcast";
}
