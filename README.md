# Hazelcast Jet Demo Applications

These are Demonstration applications using Hazelcast Jet. Each is a full
application and demonstrates how you can use Jet to solve real-world
problems.

For smaller, feature specific samples see [Jet Code
Samples](https://github.com/hazelcast/hazelcast-jet-code-samples)

## Application Demos

* [Bitcoin Death Cross](./bitcoin-death-cross) - Predicting 2018's
  downward trend for Bitcoin.
* [Real-time Image Recognition](./realtime-image-recognition) -
  Recognizes images present in the webcam video input with a model
  trained with CIFAR-10 dataset.
* [Twitter Cryptocurrency Sentiment
  Analysis](./cryptocurrency-sentiment-analysis) - Twitter content is
  analyzed in real time to calculate cryptocurrency trend list with
  popularity index.
* [Real-Time Road Traffic Analysis and
  Prediction](./road-traffic-predictor) - Continuously computes linear
  regression models from current traffic. Uses the trend from week ago
  to predict traffic now.
* [Flight Telemetry](./flight-telemetry) - Reads a stream of telemetry
  data from ADB-S on all commercial aircraft flying anywhere in the
  world. There is typically 5,000 - 6,000 aircraft at any point in time.
  This is then filtered, aggregated and certain features are enriched
  and displayed in Grafana.
* [Markov Chain Generator](./markov-chain-generator) Generates a Markov
  Chain with probabilities based on supplied classical books.
* [Train Track](./train-track) An [Apache
  Beam](https://beam.apache.org/) IOT example, tracking a GPS feed from
  a train.
* [TensorFlow](./tensorflow) A [TensorFlow](https://www.tensorflow.org/)
  example showcasing a ML model execution in Hazelcast Jet pipelines.
* [Debezium Change Data Capture with Apache
  Kafka](./debezium-cdc-with-kafka) A [Debezium](http://www.debezium.io)
  example showcasing consumption of CDC events from Apache Kafka in
  Hazelcast Jet pipelines.
* [Debezium Change Data Capture without Apache
  Kafka](./debezium-cdc-without-kafka) A
  [Debezium](http://www.debezium.io) example showcasing consumption of
  CDC events directly from Debezium using Kafka Connect source in
  Hazelcast Jet pipelines.

## External Demos

* [Real-Time dto.Trade
  Processing](https://github.com/oliversalmon/imcs-demo) Oliver
  Buckley-Salmon. Reads from a Kafka topic with Jet and then storage to
  HBase and Hazelcast IMDG. Shows enrichment and streaming aggregations.
  Jet 0.4.
* [Transport Tycoon Collision
  Prevention](https://github.com/vladoschreiner/transport-tycoon-demo/)
  Vladimír Schreiner. Reads a vehicle telemetry data from the running
  [Open Transport Tycoon DeLuxe](https://www.openttd.org/) game and uses
  Jet to predict and prevent collisions.

## Prerequisites

* Git Large File Storage: [Installation
  Guide](https://git-lfs.github.com/) Some of the demo applications
  includes machine learning models in their use cases. Since some
  models' size exceeds GitHub's 100MB file storage limit this repository
  uses Git LFS.
* Java Development Kit 8+: [Installation
  Guide](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html)
* Apache Maven: [Installation
  Guide](https://maven.apache.org/install.html)
