[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio.svg?style=social&label=Follow&style=for-the-badge)](https://twitter.com/strimziio)

# Kafka Quota Plugin

**This project is based on [https://github.com/lulf/kafka-static-quota-plugin](https://github.com/lulf/kafka-static-quota-plugin) and was originally created by [Ulf Lilleengen](https://github.com/lulf).**

This is a broker quota plugin for Apache Kafka to allow setting a per-broker limits statically in
the broker configuration. 

The default quota plugin in Apache Kafka will hand out a unique quota per client. This plugin will configure a total
quota independent of the number of clients. For example, if you have configured a produce
quota of 40 MB/second, and you have 10 producers running as fast as possible, they will be limited by 4 MB/second each. 

The quota distribution across clients is not static. If you have a max of 40 MB/second, 2 producers, and one of them is 
producing messages at 10 MB/second, the second producer will be throttled at 30 MB/second.

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

## Configuring

Configure Kafka to load the plugin and some plugin properties:

```
client.quota.callback.class=io.strimzi.kafka.quotas.StaticQuotaCallback

# The quota is given in bytes, and will translate to bytes/sec in total for your clients.
client.quota.callback.static.produce=1000000
client.quota.callback.static.fetch=1000000

# Storage quota settings in bytes. Clients will be throttled linearly between produce quota and 0
# after soft limit.
client.quota.callback.static.storage.soft=10000000
client.quota.callback.static.storage.hard=20000000
# Check storage usage every 5 seconds
client.quota.callback.static.storage.check-interval=5
```

## Testing locally

Run it locally (make sure your server.properties enables the reporter):

```
CLASSPATH=/path/to/target/kafka-quotas-plugin.jar ./bin/kafka-server-start.sh server.properties
```
