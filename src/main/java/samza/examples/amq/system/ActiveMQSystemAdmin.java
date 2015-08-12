/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package samza.examples.amq.system;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * ActiveMQ System admin
 */
public class ActiveMQSystemAdmin implements SystemAdmin {

    private static final SystemStreamMetadata.SystemStreamPartitionMetadata emptyMetadata =
            new SystemStreamMetadata.SystemStreamPartitionMetadata(null, null, null);

    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> systemStreamPartitionStringMap) {
        return null;
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> queuesNames) {
        Map<String, SystemStreamMetadata> metadata = new HashMap<String, SystemStreamMetadata>();
        int partition = 0;
        // mapping one queue to one partition
        // TODO if different ActiveMQ topology used, can we do a more clever partition?
        for (String queueName : queuesNames) {
            System.out.println("****************************");
            System.out.println(queueName);
            System.out.println("****************************");
            Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMeta = new HashMap<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata>();
            partitionMeta.put(new Partition(partition), emptyMetadata);
            metadata.put(queueName, new SystemStreamMetadata(queueName, partitionMeta));
            partition++;
        }
        return metadata;
    }

    @Override
    public void createChangelogStream(String s, int i) {

    }
}
