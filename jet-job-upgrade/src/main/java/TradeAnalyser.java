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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.pipeline.WindowDefinition;
import dto.Trade;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TradeAnalyser {

    private static final String INFLUXDB_URL = "http://localhost:8086";
    private static final String DATABASE_NAME = "trades";
    private static final String INFLUXDB_USER_NAME = "root";
    private static final String INFLUXDB_PASSWORD = "root";
    private static final String KAFKA_BROKER = "localhost:9092";

    private static Set<String> STOCKS = new HashSet<>(Arrays.asList("AAPL", "MSFT", "AMZN"));

    public static void main(String[] args) {

        Pipeline p = buildPipeline();
        JetInstance jet = Jet.bootstrappedInstance();

        try {
            JobConfig jobConfig = new JobConfig()
                    .setAutoScaling(true)
                    .setName("TradeAnalyser")
                    .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);

            jet.newJobIfAbsent(p, jobConfig).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        StreamStage<Trade> source = p
                .readFrom(KafkaSources.<String, String, Trade>kafka(
                        kafkaProps(), rec -> new Trade(rec.value()), "trades")
                )
                .withTimestamps(Trade::getTime, 0L)
                .filter(trade -> STOCKS.contains(trade.getSymbol()));

        StreamStageWithKey<Trade, String> grouped = source
                .groupingKey(Trade::getSymbol);

        StreamStage<Point> avg1s = grouped
                .window(WindowDefinition.tumbling(1_000))
                .aggregate(AggregateOperations.averagingLong(Trade::getPrice))
                .setName("avg-1s")
                .map(res -> mapToPoint("avg_1s", res))
                .setName("map-to-point");

//        StreamStage<Point> avg1m = grouped
//                .window(WindowDefinition.sliding(60_000, 1_000))
//                .aggregate(AggregateOperations.averagingLong(Trade::getPrice))
//                .setName("avg-1m")
//                .map(res -> mapToPoint("avg_1m", res))
//                .setName("map-to-point");
//
//        StreamStage<Point> vol1s = source
//                .window(WindowDefinition.tumbling(1_000))
//                .aggregate(AggregateOperations.summingLong(Trade::getQuantity))
//                .map(res -> mapToPoint("vol_1s", res))
//                .setName("vol-1s")
//                .setName("map-to-point");

        Sink<Point> influxDbSink = influxDbSink(INFLUXDB_URL, INFLUXDB_USER_NAME, INFLUXDB_PASSWORD, DATABASE_NAME);

        avg1s.writeTo(influxDbSink);
//        p.writeTo(influxDbSink, avg1m, avg1s, vol1s);

        return p;
    }

    private static Properties kafkaProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BROKER);
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        return props;
    }

    private static Point mapToPoint(String measurementName, KeyedWindowResult<String, Double> res) {
        return Point.measurement(measurementName)
                    .tag("symbol", res.key())
                    .addField("value", res.result())
                    .time(res.end(), TimeUnit.MILLISECONDS)
                    .build();
    }

    private static Point mapToPoint(String measurementName, WindowResult<Long> res) {
        return Point.measurement(measurementName)
                    .addField("value", res.result())
                    .time(res.end(), TimeUnit.MILLISECONDS)
                    .build();
    }

    private static Sink<Point> influxDbSink(String url, String username, String password, String database) {
        return SinkBuilder.sinkBuilder("influxDb-" + database,
                ctx -> InfluxDBFactory.connect(url, username, password)
                                      .setDatabase(database)
                                      .enableBatch()
        ).<Point>receiveFn(InfluxDB::write)
                .flushFn(InfluxDB::flush)
                .destroyFn(InfluxDB::close)
                .build();
    }
}
