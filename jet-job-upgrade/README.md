# Jet Job Upgrade Demo

## Summary

This demo contains a trade analyzer data pipeline that aims to depict
the main operational features of Hazelcast Jet:

- Job Upgrades
- Cluster Elasticity
- Lossless Cluster Restart
- Management tools: Management Center and Jet CLI

To demonstrate the operation safety of Hazelcast Jet, we prefer a
complex pipeline including many other applications such that Kafka,
InfluxDB, and Grafana.

### Data Pipeline

Jet job analyses trade events to compute moving average price and total
volume of trades over various time windows.

Trades are generated and stored to a Kafka topic (10k trades/sec). Jet
job reads the trades from Kafka and processes it. Results are streamed
to InfluxDB and plotted as line charts in a Grafana dashboard.

### Job Upgrades

Jet allows you to upgrade a running job while preserving its state. This
is useful for bug fixing or to deploy a new version of the application.

We will add new calculations to the running job. The first version of
the job produces just the average price over a one-second window.
Upgraded version them adds a one-minute average and one-second volume of
trades.

The one-second average calculation keeps running after the upgrade
without data loss or data corruption.

To learn more please see the [reference
manual](https://jet-start.sh/docs/enterprise/job-update).

### Cluster Elasticity

Jet cluster is elastic. It keeps processing data without loss even if a
node fails, and you can add more nodes that immediately start sharing
the computation load. Streaming jobs tend to be long-running tasks. The
elasticity of the Jet cluster allows scaling up and down with the load
to cover load spikes and to prevent overprovisioning.

The demo shows how to add or remove the node from the running cluster.

### Lossless Cluster Restart

The elasticity feature described in the previous section is entirely
RAM-based, the data is replicated in cluster memory. Given the
redundancy present in the cluster, this is sufficient to maintain a
running cluster across single-node failures (or multiple-node, depending
on the backup count), but it doesn’t cover the case when the entire
cluster must shut down.

[Lossless Cluster
Restart](https://jet-start.sh/docs/enterprise/lossless-restart) feature
allows you to gracefully shut down the cluster at any time and have all
the jobs preserved consistently. After you restart the cluster, Jet
automatically restores the data and resumes the jobs.

### Management Center and Jet CLI

[Management
 Center](https://jet-start.sh/docs/enterprise/management-center)
 is an UI to monitor and manage Jet. `Jet CLI` is a command-line
 tool to deploy and manage jobs.

We'll use both to deploy and upgrade new jobs and control the cluster.

## Code Structure

The `TradeAnalyser` class contains the analytical job. It uses
`JetBootstrap` to [submit the
job](https://jet-start.sh/docs/get-started/submit-job#submit-to-the-cluster)
to the cluster from the command line.

The `TradeProducer` class randomly generates Trades and sends them to
Kafka.

### Prerequisites

#### Hazelcast Jet Enterprise

Download [Hazelcast Jet Enterprise
4.x](https://jet-start.sh/docs/enterprise/installation) and unzip it.

Insert the enterprise license key to
`${JET_HOME}/config/hazelcast.yaml`:

```yaml
hazelcast:
  license-key: #<enter license key>
```

You can get a trial license key from
[Hazelcast](https://hazelcast.com/download/).

Enable Lossless Restart in `${JET_HOME}/config/hazelcast-jet.yaml` by
setting the `lossless-restart-enabled` to `true`:

```yaml
hazelcast-jet:
  instance:
    lossless-restart-enabled: true
```

Start Jet cluster member:

```bash
${JET_HOME}/bin/jet-start
```

#### Management Center

Management Center is distributed with Hazelcast Jet Enterprise. One has
to configure the enterprise license to use Management Center for
clusters larger than one node.

Download [Management Center
4.x](https://jet-start.sh/docs/enterprise/management-center)

Insert the license key to
`${JET_HOME}/hazelcast-jet-management-center/application.properties`.
Use the same license key as for Hazelcast Jet.  

Start Management Center for Jet:

```bash
${JET_HOME}/hazelcast-jet-management-center/jet-management-center.sh
```

Navigate your web browser to the Management Center
(<http://localhost:8081/> by default) to see the running Jet cluster:

![Management Center](/img/management-center.png)  

#### Kafka

Install [Apache Kafka](https://kafka.apache.org/) 1.x or 2.x.

Since Kafka uses ZooKeeper, you have to launch a ZooKeeper server first:

```bash
 bin/zookeeper-server-start.sh config/zookeeper.properties
```

Then, Start the Kafka server with its default properties:

```bash
bin/kafka-server-start.sh config/server.properties
```

Create the `trades` topic in Kafka:

```bash
kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1  --topic trades
```

For additional information about starting a Kafka server look at
[this](https://kafka.apache.org/quickstart)

#### InfluxDB

Install
[InfluxDB](https://www.influxdata.com/products/influxdb-overview/) 1.x.

Start InfluxDB:

```bash
influxd
```

Create the `trades` database using the `influx` tool:

```bash
influx
CREATE DATABASE trades
```

#### Grafana

Install [Grafana](https://grafana.com/) 6.x

Navigate your web browser to the Grafana UI and create an InfluxDB data
source named InfluxDB, with default URL and default password
(root:root).

Create the dashboard by importing
`grafana/trade-analysis-dashboard.json`

You should see a screen similar to this:

![Grafana dashboard](/img/grafana-default-screen.png)

### Building the Application

To build and package the application, run:

```bash
mvn clean package
```

### Running the Application

#### Start producing Trades

This shortcut starts the TradeProducer

```bash
mvn exec:java
```

It keeps generating trades at the rate of 10k trades/sec.

#### Start the Job

Deploy TradeAnalyser Jet job:

```bash
${JET_HOME}/bin/jet submit -n TradeAnalyserVersion1 target/trade-analyser-4.1.1-SNAPSHOT.jar
```

Job starts running as soon as it's deployed.

The Grafana dashboard now plots the one second moving averages for 3
data lines (3 trade symbols):

![First Job version](/img/job-version-1.png)

#### Upgrade the Job

To demonstrate job upgrade feature, change the Trade Analyser code by
enabling two more computations. Uncomment following lines in the
`src/main/java/TradeAnalyser.java`:

```java
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
```

Also, the sink definition must be changed in the code to sink results of
added computations to InfluxDB. Disable original sink and enable
`writeTo` that includes the new computations:

```java
//        avg1s.writeTo(influxDbSink);
p.writeTo(influxDbSink, avg1m, avg1s, vol1s);
```

Build the changed TradeAnalyser:

```bash
mvn clean package
```

Upgrade the running Jet job with the new version:

```bash
${JET_HOME}/bin/jet save-snapshot --cancel TradeAnalyserVersion1 SavePoint1
${JET_HOME}/bin/jet submit -n TradeAnalyserVersion2 -s SavePoint1 target/trade-analyser-4.1-SNAPSHOT.jar
```

You will see new data points in the chart as soon as the new job version
is deployed.

![First Job version](/img/job-version-2.png)

You may also inspect the execution graph of the upgraded job version in
the Management Center.

### Lossless Restart

Shutdown the cluster gracefully. Graceful shutdown waits for cluster
state to be saved to the disk.

```bash
${JET_HOME}/bin/jet-cluster-admin -o shutdown -c jet -P jet-pass
```

Alternatively, you can shutdown the cluster using the Jet Management
Center: You can inspect the Grafana dashboard - no new data is coming.

Start the cluster node again. The job is restarted without a data loss.
The computation is restored where it left off. Jet quickly catches up
with the new trades that were added to Kafka while Jet was off.

```bash
${JET_HOME}/bin/jet-start
```

## Elastic scaling

Add a member to the running cluster:

```bash
${JET_HOME}/bin/jet-start
```

The member joins the cluster and the computation is rescaled
automatically to make use of the added resources. Check the cluster
state from the command line or in the Management Center:

```bash
${JET_HOME}/bin/jet cluster
```

You should see the logs like below:

```bash
State: ACTIVE
Version: 4.1.1
Size: 2

ADDRESS                  UUID
[localhost]:5701      0ff395e2-bee6-403d-9c42-f581eab1fa90
[localhost]:5702      0333578e-3e1e-4650-b765-89e49d19c94c
```

Now you can remove a cluster member by simply killing one of the Jet
cluster processes by `CTRL-C`. The fault-tolerance mechanism kicks in
and the computation continues without data loss.
