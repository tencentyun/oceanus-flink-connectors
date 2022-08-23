/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl;

import org.apache.flink.connector.pulsar.common.request.PulsarAdminRequest;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isPartitioned;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;

/** the implements of consuming multiple topics. */
public class TopicListSubscriber extends BasePulsarSubscriber {
    private static final long serialVersionUID = 6473918213832993116L;

    private final List<String> topics;
    private final List<String> partitions;

    public TopicListSubscriber(List<String> topics) {
        ImmutableList.Builder<String> topicsBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> partitionsBuilder = ImmutableList.builder();

        for (String topic : topics) {
            if (isPartitioned(topic)) {
                partitionsBuilder.add(topic);
            } else {
                topicsBuilder.add(topic);
            }
        }

        this.topics = topicsBuilder.build();
        this.partitions = partitionsBuilder.build();
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(
            PulsarAdminRequest metadataRequest, RangeGenerator rangeGenerator, int parallelism) {
        Stream<TopicMetadata> partitionStream =
                partitions
                        .parallelStream()
                        .map(partition -> new TopicMetadata(partition, NON_PARTITIONED));
        Stream<TopicMetadata> topicStream =
                topics.parallelStream()
                        .map(topic -> queryTopicMetadata(metadataRequest, topic))
                        .filter(Objects::nonNull);

        return Stream.concat(partitionStream, topicStream)
                .flatMap(
                        metadata -> {
                            List<TopicRange> ranges = rangeGenerator.range(metadata, parallelism);
                            return toTopicPartitions(metadata, ranges).stream();
                        })
                .collect(toSet());
    }
}
