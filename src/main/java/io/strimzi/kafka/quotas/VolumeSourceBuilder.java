/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.LogDirDescription;

/**
 * A builder which ensures the <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-827%3A+Expose+log+dirs+total+and+usable+space+via+Kafka+API">KIP-827 API</a> is available and will throw exceptions if not.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-827%3A+Expose+log+dirs+total+and+usable+space+via+Kafka+API">KIP-827 API</a>
 */
public class VolumeSourceBuilder implements AutoCloseable {

    private final Supplier<Boolean> kip827Available;
    private final Function<StaticQuotaConfig.KafkaClientConfig, Admin> adminClientFactory;
    private Admin adminClient;
    private StaticQuotaConfig config;
    private Consumer<Collection<Volume>> volumesConsumer;

    /**
     * Default production constructor for production usage.
     * Which will lazily create a Kafka admin client using the supplied config.
     */
    @SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR") //false positive we are just passing the method reference
    public VolumeSourceBuilder() {
        this(VolumeSourceBuilder::testForKip827, kafkaClientConfig -> AdminClient.create(kafkaClientConfig.getKafkaClientConfig()));
    }

    /**
     * Secondary constructor visible for testing.
     * @param kip827Available used to determine if KIP-827 API's are available
     * @param adminClientFactory factory function for creating Admin clients with the builders' config.
     */
    /* test */ VolumeSourceBuilder(Supplier<Boolean> kip827Available, Function<StaticQuotaConfig.KafkaClientConfig, Admin> adminClientFactory) {
        this.kip827Available = kip827Available;
        this.adminClientFactory = adminClientFactory;
    }

     /*test*/ static Boolean testForKip827() {
        try {
            LogDirDescription.class.getDeclaredMethod("totalBytes");
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
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
     *
     * @param volumesConsumer The volume consumer to register for updates.
     * @return this to allow fluent usage of the builder.
     */
    public VolumeSourceBuilder withVolumeConsumer(Consumer<Collection<Volume>> volumesConsumer) {
        this.volumesConsumer = volumesConsumer;
        return this;
    }

    Runnable build() {
        if (!kip827Available.get()) {
            throw new IllegalStateException("KIP-827 not available, this plugin requires broker version >= 3.3");
        }
        adminClient = adminClientFactory.apply(config.getKafkaClientConfig());
        return new ClusterVolumeSource(adminClient, volumesConsumer);
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
