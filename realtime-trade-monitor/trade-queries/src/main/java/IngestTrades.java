import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;

public class IngestTrades {

    public static final String TOPIC = "trades";

    public static void ingestTrades(JetInstance jet, String servers) {
        try {
            JobConfig ingestTradesConfig = new JobConfig()
                    .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                    .setName("ingestTrades")
                    .addClass(IngestTrades.class);

            jet.newJobIfAbsent(createPipeline(servers), ingestTradesConfig);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline createPipeline(String servers) {
        Pipeline p = Pipeline.create();

        p.readFrom(KafkaSources.<String, String, Entry<String, HazelcastJsonValue>>kafka(kafkaSourceProps(servers),
                record -> entry(record.key(), new HazelcastJsonValue(record.value())), TOPIC)
        )
         .withoutTimestamps()
         .setLocalParallelism(2)
         .writeTo(Sinks.map("trades"));

        return p;
    }

    private static Properties kafkaSourceProps(String servers) {
        Properties props = new Properties();
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("bootstrap.servers", servers);
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        return props;
    }

}
