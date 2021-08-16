/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StaticQuotaCallbackTest {

    StaticQuotaCallback target;

    @BeforeEach
    void setup() {
        target = new StaticQuotaCallback();
    }

    private static void failValidator(StaticQuotaConfig.PercentagesValidator validator, String config) {
        assertThrows(InvalidConfigurationException.class, () -> validator.ensureValid("test", config));
    }

    @Test
    void testPercentagesValidator() throws IOException {
        StaticQuotaConfig.PercentagesValidator validator = new StaticQuotaConfig.PercentagesValidator();

        validator.ensureValid("test", "/path;80;99");
        validator.ensureValid("test", "/path;80.1;99");
        validator.ensureValid("test", "/path;80.1;99.9");
        validator.ensureValid("test", "/path1;80.1;99.9|/path2;70;100.0");

        failValidator(validator, "80;100");
        failValidator(validator, ";80;100");
        failValidator(validator, "/path;80");
        failValidator(validator, "/path;80;");
        failValidator(validator, "/path;80;90;112");

        Path temp = Files.createTempDirectory("validator");
        String logDir = temp.toAbsolutePath().toString();
        assertThrows(
            InvalidConfigurationException.class,
            () -> target.configure(Map.of("log.dirs", logDir)) // missing percentages config with logDir
        );
        assertThrows(
            InvalidConfigurationException.class,
            // soft > hard
            () -> target.configure(Map.of("log.dirs", logDir, StaticQuotaConfig.STORAGE_QUOTA_PERCENTAGES_PROP, logDir + ";80;70"))
        );
        assertThrows(
            InvalidConfigurationException.class,
            // hard > 100
            () -> target.configure(Map.of("log.dirs", logDir, StaticQuotaConfig.STORAGE_QUOTA_PERCENTAGES_PROP, logDir + ";80;101"))
        );
    }

    @Test
    void testStorageCheckCheckDiskUsageZeroWhenMissing() throws IOException {
        Path temp = Files.createTempDirectory("checkDiskUsage");
        String logDir = temp.toAbsolutePath().toString();
        target.configure(Map.of("log.dirs", logDir, StaticQuotaConfig.STORAGE_QUOTA_PERCENTAGES_PROP, logDir + ";80;100"));
        Files.delete(temp);
        assertEquals(0, target.storageChecker.checkDiskUsage(new HashMap<>()));
    }

    @Test
    void testStorageCheckCheckDiskUsageAtLeastFileSize() throws IOException {
        Path tempDir = Files.createTempDirectory("checkDiskUsage");
        Path tempFile = Files.createTempFile(tempDir, "t", ".tmp");
        String logDir = tempDir.toAbsolutePath().toString();
        target.configure(Map.of("log.dirs", logDir, StaticQuotaConfig.STORAGE_QUOTA_PERCENTAGES_PROP, logDir + ";80;100"));

        try {
            Files.writeString(tempFile, "0123456789");
            long minSize = Files.size(tempFile);
            Map<FileStore, Double> volumePercents = new HashMap<>();
            assertTrue(target.storageChecker.checkDiskUsage(volumePercents) >= minSize);
            assertTrue(volumePercents.size() > 0);
        } finally {
            Files.delete(tempFile);
            Files.delete(tempDir);
        }
    }

    @Test
    void testStorageCheckCheckDiskUsageNotDoubled() throws IOException {
        Path tempDir1 = Files.createTempDirectory("checkDiskUsage");
        Path tempDir2 = Files.createTempDirectory("checkDiskUsage");
        Path logDir1 = tempDir1.toAbsolutePath();
        Path logDir2 = tempDir2.toAbsolutePath();
        String logDirs = String.format("%s,%s", logDir1, logDir2);
        target.configure(Map.of("log.dirs", logDirs, StaticQuotaConfig.STORAGE_QUOTA_PERCENTAGES_PROP, String.format("%s;80;100|%s;70;99", logDir1, logDir2)));

        try {
            FileStore store = Files.getFileStore(tempDir1);
            assertEquals(store.getTotalSpace() - store.getUsableSpace(), target.storageChecker.checkDiskUsage(new HashMap<>()));
        } finally {
            Files.delete(tempDir1);
            Files.delete(tempDir2);
        }
    }
}
