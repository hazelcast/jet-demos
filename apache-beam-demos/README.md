# Hazelcast Jet Demo Applications - Apache Beam

[Apache Beam](https://beam.apache.org/) is a standard framework for *B*atch and str*EAM* processing.

You define your processing using a standardised interface, and submit it to a "_Beam runner_" for execution.

[Hazelcast Jet](https://jet-start.sh/) is a Beam runner, an implementation of the Beam standard.
Other implementations exist obviously, but they need not support all features of the Beam standard.
See [Beam Capability Matrix](https://beam.apache.org/documentation/runners/capability-matrix/).

## Application Demos

* [Train Track](./train-track) An IOT example, tracking a GPS feed from a train.