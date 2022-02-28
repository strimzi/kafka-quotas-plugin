/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StorageCheckerTest {

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
    void storageCheckCheckDiskUsageZeroWhenMissing() throws Exception {
        target.configure(0, List.of(tempDir), storage -> { });
        Files.delete(tempDir);
        assertEquals(0, target.checkDiskUsage());
    }

    @Test
    void storageCheckCheckDiskUsageAtLeastFileSize() throws Exception {
        Path tempFile = Files.createTempFile(tempDir, "t", ".tmp");
        target.configure(0, List.of(tempDir), storage -> { });

        Files.writeString(tempFile, "0123456789");
        long minSize = Files.size(tempFile);
        assertTrue(target.checkDiskUsage() >= minSize);
    }

    @Test
    void storageCheckCheckDiskUsageNotDoubled(@TempDir Path tempDir1, @TempDir Path tempDir2) throws Exception {
        target.configure(0, List.of(tempDir1, tempDir2), storage -> { });

        FileStore store = Files.getFileStore(tempDir1);
        assertEquals(store.getTotalSpace() - store.getUsableSpace(), target.checkDiskUsage());
    }

    @Test
    void testStorageCheckerEmitsUsedStorageValue() throws Exception {
        Path tempFile = Files.createTempFile(tempDir, "t", ".tmp");
        Files.writeString(tempFile, "0123456789");
        long minSize = Files.size(tempFile);

        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        target.configure(25, List.of(tempDir), completableFuture::complete);
        target.startIfNecessary();

        Long storage = completableFuture.get(1, TimeUnit.SECONDS);
        assertTrue(storage >= minSize);
    }
}
