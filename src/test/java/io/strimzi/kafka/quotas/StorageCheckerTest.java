/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import static io.strimzi.kafka.quotas.StaticQuotaCallbackTest.getMetricGroup;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(InMemoryFileStoreExtension.class)
public class StorageCheckerTest {

    private static final String MBEAN_NAME_FORMAT = "io.strimzi.kafka.quotas:type=StorageChecker,name=StorageUsedBytes,volume=%s";
    StorageChecker target;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setup() {
        target = new StorageChecker();
    }

    @AfterEach
    void teardown() throws Exception {
        if (target != null) {
            target.stop();
        }
    }

    @Test
    void shouldUseErrorMarkerWhenMissingLogDir() throws Exception {
        //Given
        target.configure(0, List.of(tempDir), diskUsage -> { });
        final String diskName = Files.getFileStore(tempDir).name();
        Files.delete(tempDir);

        //When
        final Long diskUsage = target.gatherDiskUsage().get(diskName);

        //Then
        assertEquals(-1, diskUsage);
    }

    @Test
    void shouldReturnAtLeastFileSize() throws Exception {
        //Given
        target.configure(0, List.of(tempDir), diskUsage -> { });
        long minSize =  prepareFileStore(tempDir, "0123456789");

        final String diskName = Files.getFileStore(tempDir).name();

        //When
        final Long diskUsage = target.gatherDiskUsage().get(diskName);

        //Then
        assertTrue(diskUsage >= minSize);
    }

    @Test
    void shouldGetDiskUsageFromMultipleLogDirs(@InMemoryFileStoreExtension.InMemoryFileStore Path store1, @TempDir Path store2) throws Exception {
        //Given
        target.configure(0, List.of(store1, store2), diskUsage -> { });

        long store1Size = prepareFileStore(store1, "0123456789");
        long store2Size = prepareFileStore(store2, "01234567893423543534");

        //When
        final Map<String, Long> diskUsage = target.gatherDiskUsage();

        //Then
        assertTrue(diskUsage.get(Files.getFileStore(store1).name()) >= store1Size);
        assertTrue(diskUsage.get(Files.getFileStore(store2).name()) >= store2Size);
    }

    @Test
    void testStorageCheckerEmitsUsedStorage() throws Exception {
        long minSize = prepareFileStore(tempDir, "0123456789");

        CompletableFuture<Map<String, Long>> completableFuture = new CompletableFuture<>();
        target.configure(25, List.of(tempDir), completableFuture::complete);
        target.startIfNecessary();

        Map<String, Long> storagePerDisk = completableFuture.get(1, TimeUnit.SECONDS);

        assertTrue(storagePerDisk.getOrDefault(Files.getFileStore(tempDir.toAbsolutePath()).name(), 0L) >= minSize);
    }

    @Test
    void shouldCreateMetricsPerVolume(@InMemoryFileStoreExtension.InMemoryFileStore Path store1) throws IOException {
        //Given
        final String tempDirVolumeName = Files.getFileStore(tempDir).name();
        final String store1Name = Files.getFileStore(store1).name();

        //When
        target.configure(0, List.of(tempDir, store1), diskUsage -> { });

        //Then
        SortedMap<MetricName, Metric> group = getMetricGroup("io.strimzi.kafka.quotas.StaticQuotaCallback", "StorageChecker");

        // the mbean name is part of the public api
        Set<String> names = group.keySet().stream().map(MetricName::getMBeanName).collect(Collectors.toSet());
        assertTrue(names.contains(String.format(MBEAN_NAME_FORMAT, tempDirVolumeName)), "mbean for " + tempDirVolumeName + " missing");
        assertTrue(names.contains(String.format(MBEAN_NAME_FORMAT, store1Name)), "mbean for " + store1Name + " missing");
    }

    private long prepareFileStore(Path fileStorePath, String fileContent) throws IOException {
        Path file = Files.createTempFile(fileStorePath, "t", ".tmp");
        Files.writeString(file, fileContent);
        return Files.size(file);
    }
}
