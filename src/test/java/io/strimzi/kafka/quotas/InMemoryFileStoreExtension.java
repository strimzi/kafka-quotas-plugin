/*
 * Copyright 2022, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.FileSystem;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class InMemoryFileStoreExtension implements ParameterResolver, AfterEachCallback {
    private final ExtensionContext.Namespace namespace = ExtensionContext.Namespace.create("FileStoreExtension");

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.findAnnotation(InMemoryFileStore.class).isPresent();
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        final FileSystem fs = (FileSystem) extensionContext.getStore(namespace).getOrComputeIfAbsent(FileSystem.class, clazzz -> Jimfs.newFileSystem("MockFileStore-" + parameterContext.getParameter().getName(), Configuration.unix()));
        return fs.getPath("/");
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        final FileSystem oldFs = (FileSystem) extensionContext.getStore(namespace).remove(FileSystem.class);
        if (oldFs != null) {
            oldFs.close();
        }
    }


    @Target({ElementType.FIELD, ElementType.PARAMETER})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface InMemoryFileStore {
    }
}
