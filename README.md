# Baryon
**Baryon is a library for building Spark streaming applications that consume data from Kafka.**

Baryon abstracts away all the bookkeeping involved in reliably connecting to a Kafka cluster and fetching data from it, so that users only need to focus on the logic to process this data.

For a detailed guide on getting started with Baryon, take a look at the [wiki](../../wiki).

## Why Baryon?
Spark itself also has libraries for interacting with Kafka, as documented in its [Kafka integration guide](https://spark.apache.org/docs/latest/streaming-kafka-integration.html). These libraries are well-developed, but there are certain limitations there that Baryon intends to address:

* [Code-independent checkpointing](Code-Independent-Checkpointing)

  Baryon's Kafka state management system allows Kafka consumption state to be stored across multiple runs of an application, even when there are code changes. Spark's checkpointing system does not support maintaining state across changes in code, so users of Spark's Kafka libraries must implement the offset management logic themselves.

* Improved error handling

  Baryon handles errors related to Kafka much more thoroughly than Spark's Kafka libraries, so users don't need to worry about handling Kafka problems in their code.


In addition to the above, there are a handful of additional features unique to Baryon:

* [Multiple consumption modes](Consumption-Modes)

  Baryon has two modes of consumption, the blocking mode and the non-blocking mode, which can be changed without any code changes. The blocking mode more or less corresponds to the consumption behavior of the ["direct" approach](https://spark.apache.org/docs/latest/streaming-kafka-integration.html#approach-2-direct-approach-no-receivers), while the non-blocking mode has consumption behavior similar to the [receiver-based approach](https://spark.apache.org/docs/latest/streaming-kafka-integration.html#approach-1-receiver-based-approach).

* [Dynamically configured topics](Dynamically-Configured-Topics)

  Baryon supports changes to the set of Kafka topics that are consumed while the application is running. Alongside this, configurations can be set at a per-topic level, which makes it easier to build a single application to process multiple, heterogeneous data streams.

* [Aggregated metrics](Aggregated-Metrics)

  Baryon uses the [spark-metrics library](https://github.com/groupon/spark-metrics) to collect and aggregate useful metrics across the driver and executors. These include metrics like offset lag, throughput, error rates, as well as augmented versions of existing metrics that Spark provides. The metrics here are integrated with Spark's metrics system, so they are compatible with the reporting system that comes with Spark.


## Quick Start
Add Baryon as a dependency:
```xml
<dependency>
    <groupId>com.groupon.dse</groupId>
    <artifactId>baryon</artifactId>
    <version>1.0</version>
</dependency>
```

If you want to add custom metrics that are integrated with Spark, use the [spark-metrics](https://github.com/groupon/spark-metrics) that Baryon also uses:
```xml
<dependency>
    <groupId>com.groupon.dse</groupId>
    <artifactId>spark-metrics</artifactId>
    <version>1.0</version>
</dependency>
```

Take a look at the [examples](src/main/scala/com/groupon/dse/example) to see how to write the driver and a `ReceiverPlugin`.

