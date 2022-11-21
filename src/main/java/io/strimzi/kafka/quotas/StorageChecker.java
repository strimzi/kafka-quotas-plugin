/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically reports the total storage used by one or more filesystems.
 */
public class StorageChecker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(StorageChecker.class);

    private volatile List<Path> logDirs;
    private volatile Consumer<Collection<Volume>> consumer;

    void configure(List<Path> logDirs, Consumer<Collection<Volume>> consumer) {
        this.logDirs = logDirs;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        if (logDirs != null && !logDirs.isEmpty()) {
            try {
                log.info("Quota Storage Checker is now starting");
                try {
                    consumer.accept(checkDiskUsage());
                } catch (Exception e) {
                    log.warn("Exception in storage checker thread", e);
                }
            } finally {
                log.info("Quota Storage Checker is now finishing");
            }
        }
    }

    Collection<Volume> checkDiskUsage() {
        return logDirs.stream()
                .filter(Files::exists)
                .map(path -> apply(() -> Files.getFileStore(path)))
                .distinct()
                .map(store -> new Volume("-1", apply(store::name), apply(store::getTotalSpace),
                        apply(store::getUsableSpace)))
                .collect(Collectors.toUnmodifiableList());
    }

    static <T> T apply(IOSupplier<T> supplier) {
        try {
            return supplier.get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @FunctionalInterface
    interface IOSupplier<T> {
        T get() throws IOException;
    }
}
