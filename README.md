[![Build Status](https://dev.azure.com/cncf/strimzi/_apis/build/status/kafka-quotas-plugin?branchName=main)](https://dev.azure.com/cncf/strimzi/_build/latest?definitionId=31&branchName=main)
[![GitHub release](https://img.shields.io/github/release/strimzi/kafka-quotas-plugin.svg)](https://github.com/strimzi/kafka-quotas-plugin/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.strimzi/kafka-quotas-plugin/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.strimzi/kafka-quotas-plugin)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio.svg?style=social&label=Follow&style=for-the-badge)](https://twitter.com/strimziio)

# Kafka Quota Plugin

**This project is based on [https://github.com/lulf/kafka-static-quota-plugin](https://github.com/lulf/kafka-static-quota-plugin) and was originally created by [Ulf Lilleengen](https://github.com/lulf).**

This is a broker quota plugin for Apache Kafka to allow setting a per-broker limits statically in the broker configuration. 

The default quota plugin in Apache Kafka will hand out a unique quota per client. 
This plugin will configure a total quota independent of the number of clients. 
For example, if you have configured a produce quota of 40 MB/second, and you have 10 producers running as fast as possible, they will be limited by 4 MB/second each. 

The quota distribution across clients is not static. 
If you have a max of 40 MB/second, 2 producers, and one of them is producing messages at 10 MB/second, the second producer will be throttled at 30 MB/second.

## Using with Strimzi

The plugin is already included in Strimzi and needs to be configured in the `Kafka` custom resource.
The following example shows the configuration:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
  labels:
    app: my-cluster
spec:
  # ...
  kafka:
    # ...
    config:
      # ...
      client.quota.callback.class: io.strimzi.kafka.quotas.StaticQuotaCallback
      client.quota.callback.static.produce: 1000000                                    # 1 MB/s
      client.quota.callback.static.fetch: 1000000                                      # 1 MB/s
      client.quota.callback.static.storage.soft: 239538204672                          # 80GB
      client.quota.callback.static.storage.hard: 249538204672                          # 100GB
      client.quota.callback.static.storage.check-interval: 5                           # Check storage every 5 seconds
      client.quota.callback.static.excluded.principal.name.list: principal1,principal2 # Optional list of principals not to be subjected to the quota
  # ...
```

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

# Storage quota settings in bytes. Clients will be throttled linearly between produce quota and 0 after soft limit.
# In this example the soft limit is set to 80GB and the hard limit to 100GB 
client.quota.callback.static.storage.soft=80000000000
client.quota.callback.static.storage.hard=100000000000

# Check storage usage every 5 seconds
client.quota.callback.static.storage.check-interval=5

# Optional list of principals not to be subjected to the quota
client.quota.callback.static.excluded.principal.name.list=principal1,principal2
```

## Metrics

The plugin currently provides 2 metrics:
* `io.strimzi.kafka.quotas:type=StorageChecker,name=TotalStorageUsedBytes` shows the current storage usage
* `io.strimzi.kafka.quotas:type=StorageChecker,name=SoftLimitBytes` shows the currently configured soft limit

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
