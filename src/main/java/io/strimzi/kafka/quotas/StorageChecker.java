/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically reports the total storage used by one or more filesystems.
 */
public class StorageChecker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(StorageChecker.class);

    private final Thread storageCheckerThread = new Thread(this, "storage-quota-checker");
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong storageUsed = new AtomicLong(0);

    private volatile long storageCheckIntervalMillis;
    private volatile List<Path> logDirs;
    private volatile Consumer<Long> totalUsageConsumer;
    private volatile Consumer<Map<String, Long>> perDiskUsageConsumer;

    void configure(long storageCheckIntervalMillis, List<Path> logDirs, Consumer<Long> totalUsageConsumer, Consumer<Map<String, Long>> perDiskUsageConsumer) {
        this.storageCheckIntervalMillis = storageCheckIntervalMillis;
        this.logDirs = logDirs;
        this.totalUsageConsumer = totalUsageConsumer;
        this.perDiskUsageConsumer = perDiskUsageConsumer;
    }

    void startIfNecessary() {
        if (running.compareAndSet(false, true) && storageCheckIntervalMillis > 0) {
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
                        final Map<String, Long> usagePerDisk = gatherDiskUsage();
                        long totalDiskUsage = totalDiskUSage(usagePerDisk);
                        long previousUsage = storageUsed.getAndSet(totalDiskUsage);
                        if (totalDiskUsage != previousUsage) {
                            totalUsageConsumer.accept(totalDiskUsage);
                            perDiskUsageConsumer.accept(usagePerDisk);
                        }
                        log.debug("Storage usage checked: {}", storageUsed.get());
                        Thread.sleep(storageCheckIntervalMillis);
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

    @Deprecated(forRemoval = true, since = "0.3.0")
    long checkDiskUsage() {
        return totalDiskUSage(gatherDiskUsage());
    }

    long totalDiskUSage(Map<String, Long> diskUsage) {
        return diskUsage.values().stream().mapToLong(Long::longValue).sum();
    }

    Map<String, Long> gatherDiskUsage() {
        return logDirs.stream()
                .filter(Files::exists)
                .map(path -> apply(() -> Files.getFileStore(path)))
                .distinct()
                .map(store -> {
                    final Long usedSpace = apply(() -> store.getTotalSpace() - store.getUsableSpace());
                    return new AbstractMap.SimpleImmutableEntry<>(store.name(), usedSpace != null ? usedSpace : 0);
                })
                .collect(Collectors.toMap(AbstractMap.SimpleImmutableEntry::getKey, AbstractMap.SimpleImmutableEntry::getValue));
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
