/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toSet;


/**
 * Leverages <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-827%3A+Expose+log+dirs+total+and+usable+space+via+Kafka+API">KIP-827</a>
 * to gather volume usage statistics for each Kafka log dir reported by the cluster.
 * <p>
 * A listener is registered with this volume source to act on the disk usage information.
 */
public class VolumeSource implements Runnable {

    private final Consumer<Collection<VolumeUsage>> volumeConsumer;
    private final Admin admin;
    private final int timeout;
    private final TimeUnit timeoutUnit;

    private static final Logger log = LoggerFactory.getLogger(VolumeSource.class);

    /**
     * @param admin          The Kafka Admin client to be used for gathering information.
     * @param volumeConsumer the listener to be notified of the volume usage
     * @param timeout        how long should we wait for cluster information
     * @param timeoutUnit    What unit is the timeout configured in
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2") //Injecting the dependency is the right move as it can be shared
    public VolumeSource(Admin admin, Consumer<Collection<VolumeUsage>> volumeConsumer, int timeout, TimeUnit timeoutUnit) {
        this.volumeConsumer = volumeConsumer;
        this.admin = admin;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    @Override
    public void run() {
        log.debug("Attempting to describe cluster");
        final DescribeClusterResult clusterResult = admin.describeCluster();
        try {
            clusterResult.nodes().whenComplete((nodes, throwable) -> {
                if (throwable != null) {
                    log.error("error while describing cluster", throwable);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Successfully described cluster: " + nodes);
                    }
                    onClusterDescribeSuccess(nodes);
                }
            }).get(timeout, timeoutUnit);
        } catch (InterruptedException e) {
            log.warn("Caught interrupt exception trying to describe cluster: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            log.warn("Caught exception trying to describe cluster: {}", e.getMessage(), e);
            //TODO should we cancel the futures here (specifically in the event of a timeout)?
        }
    }

    private void onClusterDescribeSuccess(Collection<Node> nodes) {
        final Set<Integer> allBrokerIds = nodes.stream().map(Node::id).collect(toSet());
        final DescribeLogDirsResult logDirsResult = admin.describeLogDirs(allBrokerIds);
        logDirsResult.allDescriptions().whenComplete((logDirsPerBroker, throwable) -> {
            if (throwable != null) {
                log.error("error while describing log dirs", throwable);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Successfully described logDirs: " + logDirsPerBroker);
                }
                onDescribeLogDirsSuccess(logDirsPerBroker);
            }
        });
    }

    private void onDescribeLogDirsSuccess(Map<Integer, Map<String, LogDirDescription>> logDirsPerBroker) {
        final List<VolumeUsage> volumes = logDirsPerBroker.entrySet().stream()
                .flatMap(VolumeSource::toVolumes).collect(Collectors.toUnmodifiableList());
        if (log.isDebugEnabled()) {
            log.debug("Notifying consumers of volumes: " + volumes);
        }
        volumeConsumer.accept(volumes);
    }


    private static Stream<? extends VolumeUsage> toVolumes(Map.Entry<Integer, Map<String, LogDirDescription>> brokerIdToLogDirs) {
        return brokerIdToLogDirs.getValue().entrySet().stream().map(logDirs -> {
            LogDirDescription logDirDescription = logDirs.getValue();
            final long totalBytes = logDirDescription.totalBytes().orElse(0);
            final long usableBytes = logDirDescription.usableBytes().orElse(0);
            return new VolumeUsage("" + brokerIdToLogDirs.getKey(), logDirs.getKey(), totalBytes, usableBytes);
        });
    }

}
