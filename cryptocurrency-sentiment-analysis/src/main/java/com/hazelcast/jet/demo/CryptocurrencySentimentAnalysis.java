package com.hazelcast.jet.demo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.contrib.twitter.TwitterSources;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.demo.support.CoinType;
import com.hazelcast.jet.demo.support.CryptoSentimentGui;
import com.hazelcast.jet.demo.support.SentimentAnalyzer;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.demo.support.Util.loadCredentials;
import static com.hazelcast.jet.demo.support.WinSize.FIVE_MINUTES;
import static com.hazelcast.jet.demo.support.WinSize.HALF_MINUTE;
import static com.hazelcast.jet.pipeline.Sinks.map;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

/**
 * This demo analyzes a live stream of tweets in real time to calculate
 * cryptocurrency trend list with its popularity index. It categorizes the
 * tweets by coin type (BTC, ETC, XRP, etc). Then it applies NLP sentiment
 * analysis to each tweet. This score says whether the Tweet has an overall
 * positive or negative sentiment. Jet uses Stanford NLP lib to compute it.
 * <p>
 * For each cryptocurrency, Jet aggregates scores from last 30 seconds and
 * last 5 minutes and pushes the results to an IMap. The demo starts a GUI
 * that takes the IMap data and visualizes it.
 * <p>
 * Below is a diagram of the computation stages:
 *
 *                  ┌───────────────────┐
 *                  │Twitter Data Source│
 *                  └──────────┬────────┘
 *                             │
 *                             v
 *                 ┌──────────────────────┐
 *                 │FlatMap Relevant Coins│
 *                 └──────────┬───────────┘
 *                            │
 *                            v
 *               ┌─────────────────────────┐
 *               │Calculate Sentiment Score│
 *               └─────────────┬───────────┘
 *                             │
 *                             v
 *                   ┌──────────────────┐
 *                   │Group by Coin Name│
 *                   └────┬─────────┬───┘
 *                        │         │
 *                ┌───────┘         └─────────┐
 *                │                           │
 *                v                           v
 *    ┌────────────────────────┐   ┌────────────────────────┐
 *    │    Calculate 5min      │   │    Calculate 30sec     │
 *    │Average with Event Count│   │Average with Event Count│
 *    └───────────┬────────────┘   └─────────────┬──────────┘
 *                │                              │
 *                v                              v
 *  ┌───────────────────────────┐ ┌─────────────────────────────┐
 *  │  Write results to IMap    │ │  Write results to IMap      │
 *  └───────────────────────────┘ └─────────────────────────────┘
 */
public class CryptocurrencySentimentAnalysis {

    private static final String MAP_NAME_JET_RESULTS = "jetResults";

    public static void main(String[] args) {
        System.out.println("DISCLAIMER: This is not investment advice");

        Pipeline pipeline = buildPipeline();
        // Start Jet
        JetInstance jet = Jet.newJetInstance();
        try {
            new CryptoSentimentGui(jet.getMap(MAP_NAME_JET_RESULTS));
            jet.newJob(pipeline).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Builds and returns the Pipeline which represents the actual computation.
     */
    private static Pipeline buildPipeline() {
        Pipeline pipeline = Pipeline.create();

        List<String> allCoinMarkers = Stream.of(CoinType.values())
                                            .flatMap(ct -> ct.markers().stream())
                                            .collect(toList());
        StreamStage<String> tweets = pipeline
                .readFrom(TwitterSources.timestampedStream(
                        loadCredentials(),
                        () -> new StatusesFilterEndpoint()
                                .trackTerms(allCoinMarkers)))
                .withNativeTimestamps(SECONDS.toMillis(1));
        StreamStageWithKey<Entry<CoinType, Double>, CoinType> tweetsWithSentiment = tweets
                .map(rawTweet -> JsonUtil.getString(rawTweet, "text"))
                .flatMap(CryptocurrencySentimentAnalysis::flatMapToRelevant)
                .mapUsingService(sentimentAnalyzerContext(), (analyzer, e1) ->
                        entry(e1.getKey(), analyzer.getSentimentScore(e1.getValue())))
                .filter(e -> !e.getValue().isInfinite() && !e.getValue().isNaN())
                .groupingKey(entryKey());

        AggregateOperation1<Entry<CoinType, Double>, ?, Tuple2<Double, Long>> avgAndCount =
                allOf(averagingDouble(Entry::getValue), counting());

        tweetsWithSentiment
                .window(sliding(HALF_MINUTE.durationMillis(), 200))
                .aggregate(avgAndCount)
                .map(windowResult -> entry(tuple2(windowResult.getKey(), HALF_MINUTE), windowResult.getValue()))
                .writeTo(map(MAP_NAME_JET_RESULTS));

        tweetsWithSentiment
                .window(sliding(FIVE_MINUTES.durationMillis(), 200))
                .aggregate(avgAndCount)
                .map(windowResult -> entry(tuple2(windowResult.getKey(), FIVE_MINUTES), windowResult.getValue()))
                .writeTo(map(MAP_NAME_JET_RESULTS));

        return pipeline;
    }

    @Nonnull
    private static ServiceFactory<SentimentAnalyzer, SentimentAnalyzer> sentimentAnalyzerContext() {
        return ServiceFactory.withCreateContextFn(jet -> new SentimentAnalyzer())
                             .withCreateServiceFn((context, sentimentAnalyzer) -> sentimentAnalyzer);
    }

    /**
     * Returns a traverser which flat maps each tweet to (coin, tweet) pairs
     * by finding coins relevant to this tweet
     *
     * @param text content of the tweet
     */
    private static Traverser<Entry<CoinType, String>> flatMapToRelevant(String text) {
        System.out.println(text);
        text = text.toLowerCase();
        AppendableTraverser<Entry<CoinType, String>> traverser = new AppendableTraverser<>(4);
        for (CoinType ct : CoinType.values()) {
            for (String marker : ct.markers()) {
                if (text.contains(marker.toLowerCase())) {
                    traverser.append(entry(ct, text));
                }
            }
        }
        return traverser;
    }
}
