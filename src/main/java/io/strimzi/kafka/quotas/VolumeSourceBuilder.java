/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.LogDirDescription;

public class VolumeSourceBuilder implements AutoCloseable {

    private final Supplier<Boolean> kip827Available;
    private final Function<StaticQuotaConfig.KafkaClientConfig, Admin> adminClientFactory;
    private Admin adminClient;
    private StaticQuotaConfig config;
    private Consumer<Collection<Volume>> volumesConsumer;

    public VolumeSourceBuilder() {
        this(VolumeSourceBuilder::testForKip827, kafkaClientConfig -> AdminClient.create(kafkaClientConfig.getKafkaClientConfig()));
    }

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

    public VolumeSourceBuilder withConfig(StaticQuotaConfig config) {
        this.config = config;
        return this;
    }

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
