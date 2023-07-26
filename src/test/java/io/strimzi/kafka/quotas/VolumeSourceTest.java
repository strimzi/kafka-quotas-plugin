/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import io.strimzi.kafka.quotas.VolumeUsageResult.VolumeSourceObservationStatus;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.strimzi.kafka.quotas.MetricUtils.METRICS_SCOPE;
import static io.strimzi.kafka.quotas.MetricUtils.assertGaugeMetric;
import static io.strimzi.kafka.quotas.MetricUtils.getMetricGroup;
import static io.strimzi.kafka.quotas.MetricUtils.resetMetrics;
import static io.strimzi.kafka.quotas.StaticQuotaCallback.HOST_BROKER_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VolumeSourceTest {

    private static final String LOCAL_NODE_ID = "3";
    private static final String METRICS_TYPE = "VolumeSource";
    private static final int NODE_ID = 7;
    private static final int NODE_2_ID = NODE_ID + 1;
    private static final String NODE_ID_STRING = String.valueOf(NODE_ID);
    private static final String NODE_2_ID_STRING = String.valueOf(NODE_2_ID);
    private static final String LOG_DIR_1 = "/data/Dir1";
    private static final String LOG_DIR_2 = "/data/Dir2";

    private CapturingVolumeObserver capturingVolumeObserver;
    private VolumeSource volumeSource;
    @Mock
    private Admin admin;

    private final Map<Integer, Map<String, LogDirDescription>> descriptions = new HashMap<>();

    private final List<Node> nodes = Lists.newArrayList();
    @Mock(lenient = true)
    private DescribeClusterResult describeClusterResult;

    @Mock(lenient = true)
    private DescribeLogDirsResult describeLogDirsResult;

    @BeforeEach
    void setUp() {
        resetMetrics(METRICS_SCOPE, METRICS_TYPE);
        capturingVolumeObserver = new CapturingVolumeObserver();
        final LinkedHashMap<String, String> tags = new LinkedHashMap<>();
        tags.put(HOST_BROKER_TAG, LOCAL_NODE_ID);
        volumeSource = new VolumeSource(admin, capturingVolumeObserver, 0, TimeUnit.SECONDS, tags);
        when(describeClusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(nodes));
        when(admin.describeCluster()).thenReturn(describeClusterResult);
        when(describeLogDirsResult.allDescriptions()).thenReturn(KafkaFuture.completedFuture(descriptions));
        lenient().when(admin.describeLogDirs(argThat(integers ->
                        integers.equals(nodes.stream().map(Node::id).collect(Collectors.toSet())))))
                .thenReturn(describeLogDirsResult);
    }

    @Test
    void shouldProduceFailedObservationIfDescribeClusterFails() {
        //Given
        final KafkaFutureImpl<Collection<Node>> kafkaFuture = new KafkaFutureImpl<>();
        kafkaFuture.completeExceptionally(new RuntimeException("boom"));
        when(describeClusterResult.nodes()).thenReturn(kafkaFuture);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageResult> results = capturingVolumeObserver.getActualResults();
        assertVolumeUsageStatus(results, VolumeSourceObservationStatus.DESCRIBE_CLUSTER_ERROR);
    }


    @Test
    void shouldProduceFailedObservationIfDescribeClusterThrows() {
        //Given
        when(describeClusterResult.nodes()).thenThrow(new RuntimeException("boom"));

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageResult> results = capturingVolumeObserver.getActualResults();
        assertVolumeUsageStatus(results, VolumeSourceObservationStatus.EXCEPTION);
    }


    @Test
    void shouldProduceFailedObservationOnDescribeClusterTimeout() {
        //Given
        final KafkaFutureImpl<Collection<Node>> neverEndingFuture = new KafkaFutureImpl<>();
        when(describeClusterResult.nodes()).thenReturn(neverEndingFuture);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageResult> results = capturingVolumeObserver.getActualResults();
        assertVolumeUsageStatus(results, VolumeSourceObservationStatus.SAFETY_TIMEOUT);
    }

    @Test
    void shouldProduceFailedObservationWhenDescribeClusterCompletesExceptionally() {
        //Given
        final KafkaFutureImpl<Collection<Node>> kafkaFuture = new KafkaFutureImpl<>();
        kafkaFuture.completeExceptionally(new RuntimeException("Boom"));
        when(describeClusterResult.nodes()).thenReturn(kafkaFuture);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageResult> results = capturingVolumeObserver.getActualResults();
        assertVolumeUsageStatus(results, VolumeSourceObservationStatus.DESCRIBE_CLUSTER_ERROR);
    }

    @Test
    void shouldProduceFailedObservationWhenDescribeLogDirsFutureCompletesExceptionally() {
        //Given
        final KafkaFutureImpl<Map<Integer, Map<String, LogDirDescription>>> kafkaFuture = new KafkaFutureImpl<>();
        kafkaFuture.completeExceptionally(new RuntimeException("Boom"));
        when(describeLogDirsResult.allDescriptions()).thenReturn(kafkaFuture);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageResult> results = capturingVolumeObserver.getActualResults();
        assertVolumeUsageStatus(results, VolumeSourceObservationStatus.DESCRIBE_LOG_DIR_ERROR);
    }

    @Test
    void shouldProduceCollectionOfVolumes() {
        //Given
        givenNode(NODE_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageResult> results = capturingVolumeObserver.getActualResults();
        final VolumeUsageResult onlyResult = assertVolumeUsageStatus(results, VolumeSourceObservationStatus.SUCCESS);
        assertThat(onlyResult.getVolumeUsages()).containsExactly(new VolumeUsage(NODE_ID_STRING, LOG_DIR_1, 50, 10));
    }

    @Test
    void shouldCreateConsumedBytesMetricForALogDir() {
        //Given
        givenNode(NODE_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);

        //When
        volumeSource.run();

        //Then
        final SortedMap<MetricName, Metric> volumeSourceMetrics = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(volumeSourceMetrics, "ConsumedBytes", 40L);
    }

    @Test
    void shouldTrackEvolutionOfConsumedBytesMetricForALogDir() {
        //Given
        givenNode(NODE_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);
        volumeSource.run();

        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 20);

        //When
        volumeSource.run();

        //Then
        final SortedMap<MetricName, Metric> volumeSourceMetrics = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(volumeSourceMetrics, "ConsumedBytes", 30L);
    }

    @Test
    void shouldTrackEvolutionOfAvailableBytesMetricForALogDir() {
        //Given
        givenNode(NODE_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);
        volumeSource.run();

        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 20);

        //When
        volumeSource.run();

        //Then
        final SortedMap<MetricName, Metric> volumeSourceMetrics = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(volumeSourceMetrics, "AvailableBytes", 20L);
    }

    @Test
    void shouldCreateAvailableBytesMetricForALogDir() {
        //Given
        givenNode(NODE_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);

        //When
        volumeSource.run();

        //Then
        final SortedMap<MetricName, Metric> volumeSourceMetrics = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(volumeSourceMetrics, "AvailableBytes", 10L);
    }

    @Test
    void shouldCreateConsumedBytesMetricForEachLogDir() {
        //Given
        givenNode(NODE_ID);
        givenNode(NODE_2_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);
        givenLogDirDescription(NODE_ID, LOG_DIR_2, 60, 15);
        givenLogDirDescription(NODE_2_ID, "dir3", 40, 1);

        //When
        volumeSource.run();

        //Then
        final SortedMap<MetricName, Metric> volumeSourceMetrics = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(volumeSourceMetrics, "ConsumedBytes", buildTagMap(NODE_ID, LOG_DIR_1), 40L);
        assertGaugeMetric(volumeSourceMetrics, "ConsumedBytes", buildTagMap(NODE_ID, LOG_DIR_2), 45L);
        assertGaugeMetric(volumeSourceMetrics, "ConsumedBytes", buildTagMap(NODE_2_ID, "dir3"), 39L);
    }

    @Test
    void shouldCreateAvailableBytesMetricForEachLogDir() {
        //Given
        givenNode(NODE_ID);
        givenNode(NODE_2_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);
        givenLogDirDescription(NODE_ID, LOG_DIR_2, 60, 15);
        givenLogDirDescription(NODE_2_ID, "dir3", 40, 1);

        //When
        volumeSource.run();

        //Then
        final SortedMap<MetricName, Metric> volumeSourceMetrics = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(volumeSourceMetrics, "AvailableBytes", buildTagMap(NODE_ID, LOG_DIR_1), 10L);
        assertGaugeMetric(volumeSourceMetrics, "AvailableBytes", buildTagMap(NODE_ID, LOG_DIR_2), 15L);
        assertGaugeMetric(volumeSourceMetrics, "AvailableBytes", buildTagMap(NODE_2_ID, "dir3"), 1L);
    }

    @Test
    void shouldProduceMultipleVolumesForASingleBrokerIfItHasMultipleLogDirs() {
        //Given
        givenNode(NODE_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);
        givenLogDirDescription(NODE_ID, LOG_DIR_2, 30, 5);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageResult> results = capturingVolumeObserver.getActualResults();
        final VolumeUsageResult onlyResult = assertVolumeUsageStatus(results, VolumeSourceObservationStatus.SUCCESS);
        VolumeUsage expected1 = new VolumeUsage(NODE_ID_STRING, LOG_DIR_1, 50, 10);
        VolumeUsage expected2 = new VolumeUsage(NODE_ID_STRING, LOG_DIR_2, 30, 5);
        assertThat(onlyResult.getVolumeUsages()).containsExactlyInAnyOrder(expected1, expected2);
    }

    @Test
    void shouldProduceMultipleVolumesForMultipleBrokers() {
        //Given
        givenNode(NODE_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);

        givenNode(NODE_2_ID);
        givenLogDirDescription(NODE_2_ID, LOG_DIR_1, 30, 5);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageResult> results = capturingVolumeObserver.getActualResults();
        final VolumeUsageResult onlyResult = assertVolumeUsageStatus(results, VolumeSourceObservationStatus.SUCCESS);
        VolumeUsage expected1 = new VolumeUsage(NODE_ID_STRING, LOG_DIR_1, 50, 10);
        VolumeUsage expected2 = new VolumeUsage(NODE_2_ID_STRING, LOG_DIR_1, 30, 5);
        assertThat(onlyResult.getVolumeUsages()).containsExactlyInAnyOrder(expected1, expected2);
    }

    @Test
    void shouldFilterVolumesWithoutAvailableBytes() {
        //Given
        givenNode(NODE_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, -1L, -1L);

        givenNode(NODE_2_ID);
        givenLogDirDescription(NODE_2_ID, LOG_DIR_1, 30, 5);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageResult> results = capturingVolumeObserver.getActualResults();
        final VolumeUsageResult onlyResult = assertVolumeUsageStatus(results, VolumeSourceObservationStatus.SUCCESS);
        VolumeUsage expected2 = new VolumeUsage(NODE_2_ID_STRING, LOG_DIR_1, 30, 5);
        assertThat(onlyResult.getVolumeUsages()).containsExactlyInAnyOrder(expected2);
    }

    @Test
    void shouldProduceEmptyIfDescribeLogDirsReturnsEmptyMaps() {
        //Given
        givenNode(1);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageResult> results = capturingVolumeObserver.getActualResults();
        final VolumeUsageResult onlyInvocation = assertVolumeUsageStatus(results, VolumeSourceObservationStatus.SUCCESS);
        assertThat(onlyInvocation.getVolumeUsages()).isEmpty();
    }

    @Test
    void shouldCountEachActiveBrokerInDescribeClusterResponse() {
        //Given
        givenNode(NODE_ID);
        givenNode(NODE_2_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);
        givenLogDirDescription(NODE_ID, LOG_DIR_2, 60, 15);
        givenLogDirDescription(NODE_2_ID, "dir3", 40, 1);

        //When
        volumeSource.run();

        //Then
        final SortedMap<MetricName, Metric> volumeSourceMetrics = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(volumeSourceMetrics, "ActiveBrokers", buildBasicTagMap(), 2L);
    }

    @Test
    void shouldCountEachActiveLogDirInDescribeLogDirsResponse() {
        //Given
        givenNode(NODE_ID);
        givenNode(NODE_2_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);
        givenLogDirDescription(NODE_ID, LOG_DIR_2, 60, 15);
        givenLogDirDescription(NODE_2_ID, "dir3", 40, 1);

        //When
        volumeSource.run();

        //Then
        final SortedMap<MetricName, Metric> volumeSourceMetrics = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(volumeSourceMetrics, "ActiveLogDirs", buildBasicTagMap(), 3L);
    }

    @Test
    void shouldCountEachValidLogDirInDescribeLogDirsResponse() {
        //Given
        givenNode(NODE_ID);
        givenNode(NODE_2_ID);
        givenLogDirDescription(NODE_ID, LOG_DIR_1, 50, 10);
        givenLogDirDescription(NODE_ID, LOG_DIR_2, -1, -1);
        givenLogDirDescription(NODE_2_ID, "dir3", 40, 1);

        //When
        volumeSource.run();

        //Then
        final SortedMap<MetricName, Metric> volumeSourceMetrics = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(volumeSourceMetrics, "ActiveLogDirs", buildBasicTagMap(), 2L);
    }

    private static LinkedHashMap<String, String> buildTagMap(int remoteNodeId, String logDir) {
        final LinkedHashMap<String, String> tags = buildBasicTagMap();
        tags.put(VolumeSource.REMOTE_BROKER_TAG, String.valueOf(remoteNodeId));
        tags.put(VolumeSource.LOG_DIR_TAG, logDir);
        return tags;
    }

    private static LinkedHashMap<String, String> buildBasicTagMap() {
        final LinkedHashMap<String, String> tags = new LinkedHashMap<>();
        tags.put(HOST_BROKER_TAG, LOCAL_NODE_ID);
        return tags;
    }

    private static class CapturingVolumeObserver implements VolumeObserver {
        private final List<VolumeUsageResult> actualResults = new ArrayList<>();

        @Override
        public void observeVolumeUsage(VolumeUsageResult volumes) {
            actualResults.add(volumes);
        }

        public List<VolumeUsageResult> getActualResults() {
            return actualResults;
        }
    }

    private void givenNode(int nodeId) {
        final Node singleNode = new Node(nodeId, "broker1", 9092);
        nodes.add(singleNode);
    }

    private void givenLogDirDescription(int nodeId, String logDir, long totalBytes, long usableBytes) {
        final LogDirDescription dirDescription = new LogDirDescription(null, Map.of(), totalBytes, usableBytes);
        descriptions.computeIfAbsent(nodeId, HashMap::new).put(logDir, dirDescription);
    }

    private static VolumeUsageResult assertVolumeUsageStatus(List<VolumeUsageResult> results, VolumeSourceObservationStatus expectedStatus) {
        assertThat(results).hasSize(1);
        final VolumeUsageResult onlyResult = results.get(0);
        assertThat(onlyResult.getStatus()).isEqualTo(expectedStatus);
        return onlyResult;
    }

}
