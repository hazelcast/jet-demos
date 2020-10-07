import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

public class App {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Available commands:");
            System.out.println(" load-symbols");
            System.out.println(" ingest-trades <bootstrap servers>");
            System.out.println(" aggregate-query <bootstrap servers>");
            System.out.println(" benchmark-index");
            System.out.println(" benchmark-latency");
            System.exit(1);
        }

        String command = args[0];

        JetInstance jet = Jet.newJetClient();
        try {
            if (command.equals("load-symbols")) {
                LoadSymbols.loadSymbols(jet);
            } else if (command.equals("ingest-trades")) {
                IngestTrades.ingestTrades(jet, args[1]);
            } else if (command.equals("aggregate-query")) {
                AggregateQuery.aggregateQuery(jet, args[1]);
            } else if (command.equals("benchmark-index")) {
                Benchmark.benchmark(jet);
            } else if (command.equals("benchmark-latency")) {
                BenchmarkLatency.benchmark(jet);
            }
        } finally {
            jet.shutdown();
        }
    }
}
