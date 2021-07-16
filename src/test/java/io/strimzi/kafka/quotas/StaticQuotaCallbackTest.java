/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StaticQuotaCallbackTest {

    StaticQuotaCallback target;

    @BeforeEach
    void setup() {
        target = new StaticQuotaCallback();
    }

    @Test
    void testStorageCheckCheckDiskUsageZeroWhenMissing() throws IOException {
        Path temp = Files.createTempDirectory("checkDiskUsage");
        target.configure(Map.of("log.dirs", temp.toAbsolutePath().toString()));
        Files.delete(temp);
        assertEquals(0, target.storageChecker.checkDiskUsage());
    }

    @Test
    void testStorageCheckCheckDiskUsageAtLeastFileSize() throws IOException {
        Path tempDir = Files.createTempDirectory("checkDiskUsage");
        Path tempFile = Files.createTempFile(tempDir, "t", ".tmp");
        target.configure(Map.of("log.dirs", tempDir.toAbsolutePath().toString()));

        try {
            Files.writeString(tempFile, "0123456789");
            long minSize = Files.size(tempFile);
            assertTrue(target.storageChecker.checkDiskUsage() >= minSize);
        } finally {
            Files.delete(tempFile);
            Files.delete(tempDir);
        }
    }

    @Test
    void testStorageCheckCheckDiskUsageNotDoubled() throws IOException {
        Path tempDir1 = Files.createTempDirectory("checkDiskUsage");
        Path tempDir2 = Files.createTempDirectory("checkDiskUsage");
        target.configure(Map.of("log.dirs", String.format("%s,%s", tempDir1.toAbsolutePath(), tempDir2.toAbsolutePath())));

        try {
            FileStore store = Files.getFileStore(tempDir1);
            assertEquals(store.getTotalSpace() - store.getUsableSpace(), target.storageChecker.checkDiskUsage());
        } finally {
            Files.delete(tempDir1);
            Files.delete(tempDir2);
        }
    }
}
