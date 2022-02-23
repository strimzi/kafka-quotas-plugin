/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Periodically reports the total storage used by one or more filesystems.
 */
public class StorageChecker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(StorageChecker.class);

    private final Thread storageCheckerThread = new Thread(this, "storage-quota-checker");
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong storageUsed = new AtomicLong(0);

    private volatile long storageCheckInterval;
    private volatile List<Path> logDirs;
    private volatile Consumer<Long> consumer;

    void configure(long storageCheckInterval, List<Path> logDirs, Consumer<Long> consumer) {
        this.storageCheckInterval = storageCheckInterval;
        this.logDirs = logDirs;
        this.consumer = consumer;
    }

    void startIfNecessary() {
        if (running.compareAndSet(false, true) && storageCheckInterval > 0) {
            storageCheckerThread.setDaemon(true);
            storageCheckerThread.start();
        }
    }

    void stop() throws InterruptedException {
        if (running.compareAndSet(true, false)) {
            storageCheckerThread.interrupt();
            storageCheckerThread.join();
        }
    }

    @Override
    public void run() {
        if (logDirs != null && !logDirs.isEmpty()) {
            try {
                log.info("Quota Storage Checker is now starting");
                while (running.get()) {
                    try {
                        long diskUsage = checkDiskUsage();
                        long previousUsage = storageUsed.getAndSet(diskUsage);
                        if (diskUsage != previousUsage) {
                            consumer.accept(diskUsage);
                        }
                        log.debug("Storage usage checked: {}", storageUsed.get());
                        Thread.sleep(TimeUnit.SECONDS.toMillis(storageCheckInterval));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        log.warn("Exception in storage checker thread", e);
                    }
                }
            } finally {
                log.info("Quota Storage Checker is now finishing");
            }
        }
    }

    long checkDiskUsage() {
        return logDirs.stream()
                .filter(Files::exists)
                .map(path -> apply(() -> Files.getFileStore(path)))
                .distinct()
                .mapToLong(store -> apply(() -> store.getTotalSpace() - store.getUsableSpace()))
                .sum();
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
