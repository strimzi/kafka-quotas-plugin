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
    private volatile Consumer<Map<String, Long>> perDiskUsageConsumer;

    void configure(long storageCheckIntervalMillis, List<Path> logDirs, Consumer<Map<String, Long>> perDiskUsageConsumer) {
        this.storageCheckIntervalMillis = storageCheckIntervalMillis;
        this.logDirs = logDirs;
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

    long totalDiskUsage(Map<String, Long> diskUsage) {
        return diskUsage.values().stream().mapToLong(Long::longValue).sum();
    }

    Map<String, Long> gatherDiskUsage() {
        return logDirs.stream()
                .filter(Files::exists)
                .map(path -> apply(() -> Files.getFileStore(path)))
                .distinct()
                .collect(Collectors.toMap(FileStore::name, StorageChecker::calculateUsedSpace));
    }

    private static long calculateUsedSpace(FileStore store) {
        Long usedSpace = apply(() -> store.getTotalSpace() - store.getUsableSpace());
        return usedSpace != null ? usedSpace : 0;
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
