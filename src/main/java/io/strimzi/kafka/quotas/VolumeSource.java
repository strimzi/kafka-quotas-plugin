/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.kafka.quotas.VolumeUsageResult.VolumeSourceObservationStatus;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.strimzi.kafka.quotas.VolumeUsageResult.failure;
import static io.strimzi.kafka.quotas.VolumeUsageResult.success;
import static java.util.stream.Collectors.toSet;


/**
 * Leverages <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-827%3A+Expose+log+dirs+total+and+usable+space+via+Kafka+API">KIP-827</a>
 * to gather volume usage statistics for each Kafka log dir reported by the cluster.
 * <p>
 * A listener is registered with this volume source to act on the disk usage information.
 */
public class VolumeSource implements Runnable {

    private final VolumeObserver volumeObserver;
    private final Admin admin;
    private final int timeout;
    private final TimeUnit timeoutUnit;

    private static final Logger log = LoggerFactory.getLogger(VolumeSource.class);

    /**
     * @param admin          The Kafka Admin client to be used for gathering information.
     * @param volumeObserver the listener to be notified of the volume usage
     * @param timeout        how long should we wait for cluster information
     * @param timeoutUnit    What unit is the timeout configured in
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2") //Injecting the dependency is the right move as it can be shared
    public VolumeSource(Admin admin, VolumeObserver volumeObserver, int timeout, TimeUnit timeoutUnit) {
        this.volumeObserver = volumeObserver;
        this.admin = admin;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    @Override
    public void run() {
        try {
            log.info("Updating cluster volume usage.");
            log.debug("Attempting to describe cluster");
            CompletableFuture<VolumeUsageResult> volumeUsagePromise = toResultStage(admin.describeCluster().nodes())
                    //Stay on the thread completing the future (probably the adminClient's thread) as the next thing we do is another admin API call
                    .thenCompose(this::onDescribeClusterComplete)
                    .toCompletableFuture();

            //Bring it back to the original thread to do the actual work of the plug-in.
            notifyObserver(volumeUsagePromise.get(timeout, timeoutUnit));
            log.info("Updated cluster volume usage.");
        } catch (InterruptedException e) {
            notifyObserver(failure(VolumeSourceObservationStatus.INTERRUPTED, e));
            log.warn("Caught interrupt exception trying to describe cluster and logDirs: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            notifyObserver(failure(VolumeSourceObservationStatus.EXECUTION_EXCEPTION, e));
            log.warn("Caught execution exception trying to describe cluster and logDirs: {}", e.getMessage(), e);
        } catch (TimeoutException e) {
            notifyObserver(failure(VolumeSourceObservationStatus.SAFETY_TIMEOUT, e));
            log.warn("Caught timeout exception trying to describe cluster and logDirs: {}", e.getMessage(), e);
        } catch (RuntimeException e) {
            notifyObserver(failure(VolumeSourceObservationStatus.EXCEPTION, e));
            log.warn("Caught runtime exception trying to describe cluster and logDirs: {}", e.getMessage(), e);
        }

    }

    private <T> CompletionStage<Result<T>> toResultStage(KafkaFuture<T> future) {
        return future
                .toCompletionStage()
                .thenApply(descriptions -> new Result<>(descriptions, null))
                .exceptionally(e -> new Result<>(null, e));
    }

    private void notifyObserver(VolumeUsageResult result) {
        if (log.isDebugEnabled()) {
            log.debug("Notifying consumers of volumes usage result: " + result);
        }
        volumeObserver.observeVolumeUsage(result);
    }

    private CompletionStage<VolumeUsageResult> onDescribeClusterComplete(Result<Collection<Node>> result) {
        if (result.isFailure()) {
            return CompletableFuture.completedFuture(failure(VolumeSourceObservationStatus.DESCRIBE_CLUSTER_ERROR, result.getThrowable()));
        } else {
            return onDescribeClusterSuccess(result.getValue());
        }
    }

    private CompletionStage<VolumeUsageResult> onDescribeClusterSuccess(Collection<Node> nodes) {
        final Set<Integer> allBrokerIds = nodes.stream().map(Node::id).collect(toSet());
        return toResultStage(admin.describeLogDirs(allBrokerIds).allDescriptions())
                .thenApply(VolumeSource::onDescribeLogDirComplete);
    }

    private static VolumeUsageResult onDescribeLogDirComplete(Result<Map<Integer, Map<String, LogDirDescription>>> logDirResult) {
        if (logDirResult.isFailure()) {
            return failure(VolumeSourceObservationStatus.DESCRIBE_LOG_DIR_ERROR, logDirResult.getThrowable());
        } else {
            return onDescribeLogDirSuccess(logDirResult.value);
        }
    }

    private static VolumeUsageResult onDescribeLogDirSuccess(Map<Integer, Map<String, LogDirDescription>> logDirsPerBroker) {
        if (log.isDebugEnabled()) {
            log.debug("Successfully described logDirs: " + logDirsPerBroker);
        }
        return success(logDirsPerBroker.entrySet()
                .stream()
                .flatMap(VolumeSource::toVolumes)
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableList()));
    }

    private static Stream<? extends VolumeUsage> toVolumes(Map.Entry<Integer, Map<String, LogDirDescription>> brokerIdToLogDirs) {
        return brokerIdToLogDirs.getValue().entrySet().stream().map(logDirs -> {
            LogDirDescription logDirDescription = logDirs.getValue();
            //Filter out Volumes from Brokers which do not support KIP-827
            //Possible during upgrades from versions before 3.3 to 3.3+
            if (logDirDescription.totalBytes().isPresent() && logDirDescription.usableBytes().isPresent()) {
                final long totalBytes = logDirDescription.totalBytes().getAsLong();
                final long usableBytes = logDirDescription.usableBytes().getAsLong();
                return new VolumeUsage("" + brokerIdToLogDirs.getKey(), logDirs.getKey(), totalBytes, usableBytes);
            } else {
                return null;
            }
        });
    }

    /**
     * Contains a result or exception
     * @param <T> the type of the result
     */
    public static class Result<T> {
        private final T value;
        private final Throwable throwable;

        /**
         * @param result the optional result instance.
         * @param throwable the optional throwable
         */
        public Result(T result, Throwable throwable) {
            this.value = result;
            this.throwable = throwable;
        }

        public T getValue() {
            return value;
        }

        public Throwable getThrowable() {
            return throwable;
        }


        public boolean isFailure() {
            return throwable != null;
        }
    }

}
