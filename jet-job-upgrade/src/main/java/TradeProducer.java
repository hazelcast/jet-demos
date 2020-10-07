/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import dto.Trade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class TradeProducer {

    private static final int TRADES_PER_SEC = 10_000;
    private static final int MAX_BATCH_SIZE = 16 * 1024;
    private static final int QUANTITY = 100;
    private static final String TOPIC = "trades";

    private final Map<String, Integer> tickerToPrice;
    private final KafkaProducer<String, String> producer;
    private final List<String> tickers;

    private long emitSchedule;

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        new TradeProducer(props, loadTickers()).run();
    }

    private TradeProducer(Properties props, List<String> tickers) {
        this.tickers = tickers;
        this.tickerToPrice  = tickers.stream().collect(Collectors.toMap(t -> t, t -> 2500));
        producer = new KafkaProducer<>(props);
        this.emitSchedule = System.nanoTime();
    }

    private void run() throws InterruptedException {
        while (true) {
            long interval = TimeUnit.SECONDS.toNanos(1) / TRADES_PER_SEC;
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                if (System.nanoTime() < emitSchedule) {
                    break;
                }
                String ticker = tickers.get(rnd.nextInt(tickers.size()));
                int price = tickerToPrice.compute(ticker, (t, v) -> v + rnd.nextInt(-1, 2));
                Trade trade = new Trade(System.currentTimeMillis(), ticker, rnd.nextInt(10, QUANTITY), price);
                producer.send(new ProducerRecord<>(TOPIC, trade.getSymbol(), trade.toString()));
                emitSchedule += interval;
            }
            Thread.sleep(1);
        }
    }

    private static List<String> loadTickers() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                TradeAnalyser.class.getResourceAsStream("/nasdaqlisted.txt"), UTF_8))
        ) {
            return reader.lines()
                         .skip(1)
                         .map(l -> l.split("\\|")[0])
                         .collect(toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
