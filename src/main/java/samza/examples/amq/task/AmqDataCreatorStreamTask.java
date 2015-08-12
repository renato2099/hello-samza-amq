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
package samza.examples.amq.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.nio.charset.Charset;

/**
 * Task to write to an ActiveMQ queue.
 */
public class AmqDataCreatorStreamTask implements StreamTask {
    /**
     * ActiveMQ endpoint where data will be written to.
     */
    private static final SystemStream OUTPUT_STREAM = new SystemStream("amq", "queue2");

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator taskCoordinator) throws Exception {
        String msg = new String((byte[])envelope.getMessage(), Charset.forName("UTF-8")).replace("Data", "Pata");
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, envelope.getKey(), msg.getBytes()));
    }
}
