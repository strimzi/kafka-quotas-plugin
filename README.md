[![Build Status](https://dev.azure.com/cncf/strimzi/_apis/build/status/kafka-quotas-plugin?branchName=main)](https://dev.azure.com/cncf/strimzi/_build/latest?definitionId=31&branchName=main)
[![GitHub release](https://img.shields.io/github/release/strimzi/kafka-quotas-plugin.svg)](https://github.com/strimzi/kafka-quotas-plugin/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.strimzi/kafka-quotas-plugin/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.strimzi/kafka-quotas-plugin)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio?style=social)](https://twitter.com/strimziio)

# Quota Plugin for Apache Kafka®

**This project is based on [https://github.com/lulf/kafka-static-quota-plugin](https://github.com/lulf/kafka-static-quota-plugin) and was originally created by [Ulf Lilleengen](https://github.com/lulf).**

This is a broker quota plugin for [Apache Kafka®](https://kafka.apache.org) to allow setting a per-broker limits statically in the broker configuration. 

The default quota plugin in Apache Kafka will hand out a unique quota per client. 
This plugin will configure a total quota independent of the number of clients. 
For example, if you have configured a produce quota of 40 MB/second, and you have 10 producers running as fast as possible, they will be limited by 4 MB/second each. 

The quota distribution across clients is not static. 
If you have a max of 40 MB/second, 2 producers, and one of them is producing messages at 10 MB/second, the second producer will be throttled at 30 MB/second.

## Using with Strimzi

**Warning: this version is not included in Strimzi yet, see tag [0.2.0](https://github.com/strimzi/kafka-quotas-plugin/tree/0.2.0)** for current Strimzi-compatible docs.

## Using with other Apache Kafka clusters 

The Quota plugin can be also used with non-Strimzi Apache Kafka clusters.
You have to add the plugin JAR file to the Apache Kafka broker class path for example by copying it to the `libs/` directory. 
Configure Apache Kafka brokers to load the plugin and configure it in the broker properties file:

```properties
# Enable the Quota plugin
client.quota.callback.class=io.strimzi.kafka.quotas.StaticQuotaCallback

# The quota is given in bytes, and will translate to bytes/sec in total for your clients
# In this example configuration the produce and fetch quota is set to 1MB/s
client.quota.callback.static.produce=1000000
client.quota.callback.static.fetch=1000000

client.quota.callback.static.kafka.admin.bootstrap.servers=localhost:9092 # required if storage.check-interval is >0
client.quota.callback.static.kafka.admin.ssl.truststore.location=/tmp/trust.jks # optionally configure the admin client further
# Storage quota settings in bytes. Clients will be throttled to 0 when any volume in the cluster has <= 5GB available bytes
client.quota.callback.static.storage.per.volume.limit.min.available.bytes=5368709120

# Check storage usage every 5 seconds
client.quota.callback.static.storage.check-interval=5

# Optional list of principals not to be subjected to the quota
client.quota.callback.static.excluded.principal.name.list=principal1,principal2
```

## Metrics

The plugin currently provides the following metrics:
* `io.strimzi.kafka.quotas:type=StaticQuotaCallback,name=Produce` shows the currently configured produce quota
* `io.strimzi.kafka.quotas:type=StaticQuotaCallback,name=Fetch` shows the currently configured fetch quota
* `io.strimzi.kafka.quotas:type=StaticQuotaCallback,name=Request` shows the currently configured request quota

### Additional metrics for cluster wide monitoring

| Name                          | Metric Type | Meaning                                                                        | Type           | Tags                                          |
|-------------------------------|-------------|--------------------------------------------------------------------------------|----------------|-----------------------------------------------|
| ThrottleFactor                | Gauge       | The current factor applied by the plug-in [0..1]                               | ThrottleFactor | `observingBrokerId`                           |
| FallbackThrottleFactorApplied | Counter     | The number of times the plug-in has transitioned to using the fall back factor | ThrottleFactor | `observingBrokerId`                           |
| LimitViolated                 | Counter     | A count of the number `logDir`s which violate the configured limit             | ThrottleFactor | `observingBrokerId`                           |
| ActiveBrokers                 | Gauge       | The current number of brokers returned by the describeCluster rpc              | VolumeSource   | `observingBrokerId`                           |
| ActiveLogDirs                 | Gauge       | The number of logDirs returned by the describeLogDirs RPC                      | VolumeSource   | `observingBrokerId`                           | 
| AvailableBytes                | Gauge       | The number of available bytes returned by the describeLogDirs RPC              | VolumeSource   | `[observingBrokerId, remoteBrokerId, logDir]` |
| ConsumedBytes                 | Gauge       | The number of consumed bytes returned by the describeLogDirs RPC               | VolumeSource   | `[observingBrokerId, remoteBrokerId, logDir]` |

### Tag definitions
| Tag               | Definition                                                             |
|-------------------|------------------------------------------------------------------------|
| observingBrokerId | The BrokerId of the broker node executing the plug-in                  |
| remoteBrokerId    | The BrokerId of the broker node hosting the logDir                     |
| logDir            | The path to the specific logDir as returned by the describeLogDirs RPC |

## Building

To build the plugin:

```
mvn package
```

Copy the resulting jar in `target/kafka-quotas-plugin.jar` into the Kafka classpath.

Alternatively, you can publish to your local maven repository with:

```
mvn install
```

## Testing locally

Run it locally (make sure your server.properties enables the reporter):

```
CLASSPATH=/path/to/target/kafka-quotas-plugin.jar ./bin/kafka-server-start.sh server.properties
```
