/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;

/**
 * A builder which ensures the <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-827%3A+Expose+log+dirs+total+and+usable+space+via+Kafka+API">KIP-827 API</a> is available and will throw exceptions if not.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-827%3A+Expose+log+dirs+total+and+usable+space+via+Kafka+API">KIP-827 API</a>
 */
public class VolumeSourceBuilder implements AutoCloseable {

    private final Function<StaticQuotaConfig.KafkaClientConfig, Admin> adminClientFactory;
    private Admin adminClient;
    private StaticQuotaConfig config;
    private VolumeObserver volumeObserver;

    /**
     * Default production constructor for production usage.
     * Which will lazily create a Kafka admin client using the supplied config.
     */
    @SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR") //false positive we are just passing the method reference
    public VolumeSourceBuilder() {
        this(kafkaClientConfig -> AdminClient.create(kafkaClientConfig.getKafkaClientConfig()));
    }

    /**
     * Secondary constructor visible for testing.
     * @param adminClientFactory factory function for creating Admin clients with the builders' config.
     */
    /* test */ VolumeSourceBuilder(Function<StaticQuotaConfig.KafkaClientConfig, Admin> adminClientFactory) {
        this.adminClientFactory = adminClientFactory;
    }

    /**
     *
     * @param config The plug-in configuration to use.
     * @return this to allow fluent usage of the builder.
     */
    public VolumeSourceBuilder withConfig(StaticQuotaConfig config) {
        this.config = config;
        return this;
    }

    /**
     * @param volumesObserver The volume consumer to register for updates.
     * @return this to allow fluent usage of the builder.
     */
    public VolumeSourceBuilder withVolumeObserver(VolumeObserver volumesObserver) {
        this.volumeObserver = volumesObserver;
        return this;
    }

    VolumeSource build() {
        if (!config.isSupportsKip827()) {
            throw new IllegalStateException("KIP-827 not available, this plugin requires broker version >= 3.3");
        }
        adminClient = adminClientFactory.apply(config.getKafkaClientConfig());
        //Timeout just before the next job will be scheduled to run to avoid tasks queuing on the client thread pool.
        final int timeout = config.getStorageCheckInterval() - 1;
        return new VolumeSource(adminClient, volumeObserver, timeout, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
