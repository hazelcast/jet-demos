package com.hazelcast.jet.demo;

import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * Polls the <a href="https://www.adsbexchange.com">ADS-B Exchange</a> HTTP API
 * for flight data. The API will be polled every {@code pollIntervalMillis} milliseconds.
 * <p>
 * After a successful poll, this source filters out aircraft which are missing registration number
 * and position timestamp. It will also records the latest position timestamp of the aircraft so if
 * there are no update for an aircraft it will not be emitted from this source.
 */
public class FlightDataSource {

    /**
     * See <a href="https://www.adsbexchange.com/data/">ADS-B Exchange</a> for how to
     * obtain an API key.
     */
    protected static final String API_AUTHENTICATION_KEY = "YOUR_API_KEY_HERE";

    private final URL url;
    private final long pollIntervalMillis;

    // holds a list of known aircraft, with last seen
    private final Map<Long, Long> aircraftLastSeenAt = new HashMap<>();
    private final ILogger logger;

    private long lastPoll;

    private FlightDataSource(ILogger logger, String url, long pollIntervalMillis) {
        this.logger = logger;
        try {
            this.url = new URL(url);
        } catch (MalformedURLException e) {
            throw ExceptionUtil.rethrow(e);
        }
        this.pollIntervalMillis = pollIntervalMillis;
    }

    private void fillBuffer(TimestampedSourceBuffer<Aircraft> buffer) throws IOException {
        long now = System.currentTimeMillis();
        if (now < (lastPoll + pollIntervalMillis)) {
            return;
        }
        lastPoll = now;

        List<Aircraft> aircraftList = pollForAircraft();
        aircraftList.stream()
                .filter(a -> !isNullOrEmpty(a.getReg())) // there should be a reg number
                // only add new positions to buffer
                .filter(a -> a.getPosTime() > aircraftLastSeenAt.getOrDefault(a.getId(), 0L))
                .forEach(a -> {
                    // update cache
                    aircraftLastSeenAt.put(a.getId(), a.getPosTime());
                    buffer.add(a, a.getPosTime());
                });

        logger.info("Polled " + aircraftList.size() + " aircraft, " + buffer.size() + " new positions.");
    }

    private List<Aircraft> pollForAircraft() throws IOException {
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
        StringBuilder response = new StringBuilder();
        try {
            con.setRequestMethod("GET");
            con.addRequestProperty("User-Agent", "Mozilla / 5.0 (Windows NT 6.1; WOW64) AppleWebKit / 537.36 (KHTML, like Gecko) Chrome / 40.0.2214.91 Safari / 537.36");
            con.addRequestProperty("api-auth", API_AUTHENTICATION_KEY);
            int responseCode = con.getResponseCode();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
            }
            if (responseCode != 200) {
                logger.info("API returned error: " + responseCode + " " + response);
                return Collections.emptyList();
            }
        } finally {
            con.disconnect();
        }
        return JsonUtil.parse(Aircraft.AircraftList.class, response.toString()).acList;
    }

    public static StreamSource<Aircraft> flightDataSource(String url, long pollIntervalMillis) {
        return SourceBuilder.timestampedStream("Flight Data Source",
                ctx -> new FlightDataSource(ctx.logger(), url, pollIntervalMillis))
                .fillBufferFn(FlightDataSource::fillBuffer)
                .build();

    }
}
