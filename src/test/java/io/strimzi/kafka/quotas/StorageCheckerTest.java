/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.jimfs.PathType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.google.common.jimfs.Feature.FILE_CHANNEL;
import static com.google.common.jimfs.Feature.LINKS;
import static com.google.common.jimfs.Feature.SECURE_DIRECTORY_STREAM;
import static com.google.common.jimfs.Feature.SYMBOLIC_LINKS;
import static org.assertj.core.api.Assertions.assertThat;

public class StorageCheckerTest {

    public static final Configuration UNIX_BUILDER = Configuration.builder(PathType.unix())
            .setRoots("/")
            .setWorkingDirectory("/work")
            .setAttributeViews("basic")
            .setBlockSize(1)
            .setMaxSize(50)
            .setSupportedFeatures(LINKS, SYMBOLIC_LINKS, SECURE_DIRECTORY_STREAM, FILE_CHANNEL).build();

    StorageChecker target;

    Path logDir;

    private FileSystem mockFileSystem;

    @BeforeEach
    void setup() throws IOException {
        mockFileSystem = Jimfs.newFileSystem("MockFs1", UNIX_BUILDER);
        logDir = Files.createDirectories(mockFileSystem.getPath("/logDir1"));
        target = new StorageChecker();
    }

    @AfterEach
    void teardown() throws IOException {
        if (mockFileSystem != null) {
            mockFileSystem.close();
        }
    }

    @Test
    void storageCheckCheckDiskUsageZeroWhenMissing() throws Exception {
        target.configure(List.of(logDir), storage -> {
        });
        Files.delete(logDir);
        assertThat(target.checkDiskUsage()).isEmpty();
    }

    @Test
    void storageCheckCheckDiskUsageEqualToFileSize() throws Exception {
        Path tempFile = Files.createTempFile(logDir, "t", ".tmp");
        target.configure(List.of(logDir), storage -> {
        });

        final String content = "0123456789";
        Files.writeString(tempFile, content);
        assertThat(target.checkDiskUsage()).containsOnly(new Volume("-1", Files.getFileStore(logDir).name(), 50L, 50L - content.getBytes().length));
    }

    @Test
    void storageCheckReturnsASingleVolumeIfMultiplePathsAreOnTheSameFilesystem() throws Exception {
        final Path logDir2 = Files.createDirectories(mockFileSystem.getPath("/logDir2"));
        target.configure(List.of(logDir, logDir2), storage -> {
        });
        assertThat(target.checkDiskUsage()).hasSize(1);
    }

    @Test
    void storageCheckReturnsMultipleVolumesIfTargetingDirectoriesOnMultipleFileSystems() throws Exception {
        try (final FileSystem fileSystem2 = Jimfs.newFileSystem("mockfs2", UNIX_BUILDER)) {
            final Path logDir2 = Files.createDirectories(fileSystem2.getPath("/logDir2"));
            target.configure(List.of(logDir, logDir2), storage -> {
            });
            assertThat(target.checkDiskUsage()).hasSize(2);
        }
    }
}
