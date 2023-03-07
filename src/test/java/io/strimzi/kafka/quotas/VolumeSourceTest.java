/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.strimzi.kafka.quotas.VolumeUsageObservation.VolumeSourceObservationStatus;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VolumeSourceTest {

    private CapturingVolumeObserver capturingVolumeObserver;
    private VolumeSource volumeSource;
    @Mock
    private Admin admin;

    private final Map<Integer, Map<String, LogDirDescription>> descriptions = new HashMap<>();

    private final List<Node> nodes = Lists.newArrayList();

    @BeforeEach
    void setUp() {
        capturingVolumeObserver = new CapturingVolumeObserver();
        volumeSource = new VolumeSource(admin, capturingVolumeObserver, 1, TimeUnit.SECONDS);
        final DescribeClusterResult mockDescribeClusterResult = mock(DescribeClusterResult.class);
        when(mockDescribeClusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(nodes));
        when(admin.describeCluster()).thenReturn(mockDescribeClusterResult);
        DescribeLogDirsResult mockDescribeLogDirsResult = mock(DescribeLogDirsResult.class);
        when(mockDescribeLogDirsResult.allDescriptions()).thenReturn(KafkaFuture.completedFuture(descriptions));
        when(admin.describeLogDirs(argThat(integers ->
                integers.equals(nodes.stream().map(Node::id).collect(Collectors.toSet())))))
                .thenReturn(mockDescribeLogDirsResult);
    }

    @Test
    void shouldProduceCollectionOfVolumes() {
        //Given
        final int nodeId = 1;
        givenNode(nodeId);
        givenLogDirDescription(nodeId, "dir1", 50, 10);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageObservation> results = capturingVolumeObserver.getActualResults();
        assertThat(results).hasSize(1);
        final VolumeUsageObservation onlyInvocation = results.get(0);
        assertThat(onlyInvocation.getStatus()).isEqualTo(VolumeSourceObservationStatus.SUCCESS);
        assertThat(onlyInvocation.getVolumeUsages()).containsExactly(new VolumeUsage("1", "dir1", 50, 10));
    }

    @Test
    void shouldProduceMultipleVolumesForASingleBrokerIfItHasMultipleLogDirs() {
        //Given
        final int nodeId = 1;
        givenNode(nodeId);
        givenLogDirDescription(nodeId, "dir1", 50, 10);
        givenLogDirDescription(nodeId, "dir2", 30, 5);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageObservation> results = capturingVolumeObserver.getActualResults();
        assertThat(results).hasSize(1);
        final VolumeUsageObservation onlyInvocation = results.get(0);
        assertThat(onlyInvocation.getStatus()).isEqualTo(VolumeSourceObservationStatus.SUCCESS);
        VolumeUsage expected1 = new VolumeUsage("1", "dir1", 50, 10);
        VolumeUsage expected2 = new VolumeUsage("1", "dir2", 30, 5);
        assertThat(onlyInvocation.getVolumeUsages()).containsExactlyInAnyOrder(expected1, expected2);
    }

    @Test
    void shouldProduceMultipleVolumesForMultipleBrokers() {
        //Given
        final int nodeId = 1;
        givenNode(nodeId);
        givenLogDirDescription(nodeId, "dir1", 50, 10);

        int nodeId2 = 2;
        givenNode(nodeId2);
        givenLogDirDescription(nodeId2, "dir1", 30, 5);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageObservation> results = capturingVolumeObserver.getActualResults();
        assertThat(results).hasSize(1);
        final VolumeUsageObservation onlyInvocation = results.get(0);
        assertThat(onlyInvocation.getStatus()).isEqualTo(VolumeSourceObservationStatus.SUCCESS);
        VolumeUsage expected1 = new VolumeUsage("1", "dir1", 50, 10);
        VolumeUsage expected2 = new VolumeUsage("2", "dir1", 30, 5);
        assertThat(onlyInvocation.getVolumeUsages()).containsExactlyInAnyOrder(expected1, expected2);
    }

    @Test
    void shouldFilterVolumesWithoutAvailableBytes() {
        //Given
        final int nodeId = 1;
        givenNode(nodeId);
        givenLogDirDescription(nodeId, "dir1", -1L, -1L);

        int nodeId2 = 2;
        givenNode(nodeId2);
        givenLogDirDescription(nodeId2, "dir1", 30, 5);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageObservation> results = capturingVolumeObserver.getActualResults();
        assertThat(results).hasSize(1);
        final VolumeUsageObservation onlyInvocation = results.get(0);
        assertThat(onlyInvocation.getStatus()).isEqualTo(VolumeSourceObservationStatus.SUCCESS);
        VolumeUsage expected2 = new VolumeUsage("2", "dir1", 30, 5);
        assertThat(onlyInvocation.getVolumeUsages()).containsExactlyInAnyOrder(expected2);
    }

    @Test
    void shouldProduceEmptyIfDescribeLogDirsReturnsEmptyMaps() {
        //Given
        givenNode(1);

        //When
        volumeSource.run();

        //Then
        final List<VolumeUsageObservation> results = capturingVolumeObserver.getActualResults();
        assertThat(results).hasSize(1);
        final VolumeUsageObservation onlyInvocation = results.get(0);
        assertThat(onlyInvocation.getStatus()).isEqualTo(VolumeSourceObservationStatus.SUCCESS);
        assertThat(onlyInvocation.getVolumeUsages()).isEmpty();
    }

    private static class CapturingVolumeObserver implements VolumeObserver {
        private final List<VolumeUsageObservation> actualResults = new ArrayList<>();

        @Override
        public void observeVolumeUsage(VolumeUsageObservation volumes) {
            actualResults.add(volumes);
        }

        public List<VolumeUsageObservation> getActualResults() {
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

}
