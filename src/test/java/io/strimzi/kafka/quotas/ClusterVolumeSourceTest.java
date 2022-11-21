/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterVolumeSourceTest {

    private VolumeConsumer volumeConsumer;
    private ClusterVolumeSource clusterVolumeSource;
    @Mock
    private Admin admin;

    @BeforeEach
    void setUp() {
        volumeConsumer = new VolumeConsumer();
        clusterVolumeSource = new ClusterVolumeSource(admin, volumeConsumer);
        final DescribeClusterResult mockDescribeClusterResult = Mockito.mock(DescribeClusterResult.class);
        final int nodeId = 1;
        final Node singleNode = new Node(nodeId, "broker1", 9092);
        when(mockDescribeClusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(singleNode)));
        when(admin.describeCluster()).thenReturn(mockDescribeClusterResult);

        DescribeLogDirsResult mockDescribeLogDirsResult = Mockito.mock(DescribeLogDirsResult.class);
        final LogDirDescription dirDescription = new LogDirDescription(null, Map.of(), 50, 10);
        Map<Integer, Map<String, LogDirDescription>> description = Map.of(nodeId, Map.of("dir1", dirDescription));
        when(mockDescribeLogDirsResult.allDescriptions()).thenReturn(KafkaFuture.completedFuture(description));
        when(admin.describeLogDirs(Set.of(singleNode.id()))).thenReturn(mockDescribeLogDirsResult);
    }

    @Test
    void shouldProduceCollectionOfVolumes() {
        //Given

        //When
        clusterVolumeSource.run();

        //Then
        final List<Collection<Volume>> results = volumeConsumer.getActualResults();
        Assertions.assertThat(results).hasSize(1);
        final Collection<Volume> onlyInvocation = results.get(0);
        Assertions.assertThat(onlyInvocation).containsExactly(new Volume("1", "dir1", 50, 10));
    }

    private class VolumeConsumer implements Consumer<Collection<Volume>> {
        private List<Collection<Volume>> actualResults = new ArrayList<>();

        @Override
        public void accept(Collection<Volume> volumes) {
            actualResults.add(volumes);
        }

        public List<Collection<Volume>> getActualResults() {
            return actualResults;
        }
    }
}
