/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically reports the total storage used by one or more filesystems.
 */
public class StorageChecker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(StorageChecker.class);
    public static final int UNKNOWN_USAGE_SENTINEL = -1;

    private final Thread storageCheckerThread = new Thread(this, "storage-quota-checker");
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong storageUsed = new AtomicLong(0);

    private volatile long storageCheckIntervalMillis;
    private volatile Map<String, FileStore> fileStores;
    private volatile Consumer<Map<String, VolumeDetails>> perDiskUsageConsumer;

    private final ConcurrentMap<String, VolumeDetails> diskUsage = new ConcurrentHashMap<>();

    void configure(long storageCheckIntervalMillis, List<Path> logDirs, Consumer<Map<String, VolumeDetails>> perDiskUsageConsumer) {
        this.storageCheckIntervalMillis = storageCheckIntervalMillis;
        this.perDiskUsageConsumer = perDiskUsageConsumer;

        fileStores = logDirs.stream()
                .filter(Files::exists)
                .map(path -> apply(() -> Files.getFileStore(path)))
                .collect(Collectors.toMap(FileStore::name, fileStore -> fileStore));
        fileStores.forEach((name, fs) -> buildGauge(name));
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
        if (fileStores != null && !fileStores.isEmpty()) {
            try {
                log.info("Quota Storage Checker is now starting");
                while (running.get()) {
                    try {
                        final Map<String, VolumeDetails> usagePerDisk = gatherDiskUsage();
                        diskUsage.clear();
                        diskUsage.putAll(usagePerDisk);
                        long totalDiskUsage = totalDiskUsage(usagePerDisk);
                        long previousUsage = storageUsed.getAndSet(totalDiskUsage);
                        if (totalDiskUsage != previousUsage) {
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

    long totalDiskUsage(Map<String, VolumeDetails> diskUsage) {
        return diskUsage.values().stream().mapToLong(VolumeDetails::getConsumedCapacity).sum();
    }

    Map<String, VolumeDetails> gatherDiskUsage() {
        return fileStores
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> calculateUsedSpace(entry.getValue())));
    }

    private static VolumeDetails calculateUsedSpace(FileStore store) {
        try {
            return new VolumeDetails(store.name(), store.getTotalSpace(), store.getUsableSpace());
        } catch (IOException e) {
            log.warn("unable to read disk space for " + store.name() + " due to " + e.getMessage(), e);
        }
        return new VolumeDetails(store.name(), -1L, -1L);
    }

    static <T> T apply(IOSupplier<T> supplier) {
        try {
            return supplier.get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void buildGauge(String name) {
        Metrics.newGauge(StaticQuotaCallback.metricName(StorageChecker.class, "StorageUsedBytes", Map.of("volume", name)), new NamedVolumeGauge(name));
    }

    public long getCapacity(String volumeName) {
        return 0;
    }

    @FunctionalInterface
    interface IOSupplier<T> {
        T get() throws IOException;
    }

    private class NamedVolumeGauge extends Gauge<Long> {

        private final String volumeName;

        private NamedVolumeGauge(String volumeName) {
            this.volumeName = volumeName;
        }

        @Override
        public Long value() {
            return diskUsage.get(volumeName).getConsumedCapacity();
        }
    }
}
