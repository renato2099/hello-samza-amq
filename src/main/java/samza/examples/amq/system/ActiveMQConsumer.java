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

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static samza.examples.amq.system.ActiveMQConstants.*;

/**
 * ActiveMQ consumer class
 */
public class ActiveMQConsumer extends BlockingEnvelopeMap {

    private AmqAckMode ackMode;
    private List<ActiveMQListener> amqListeners;

    /**
     * Logger for the ActiveMQConsumer
     */
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQConsumer.class);

    public ActiveMQConsumer(String aMode) {
        this.setAckMode(AmqAckMode.valueOf(aMode));
        this.amqListeners = new ArrayList<ActiveMQListener>();
    }

    public void putMessage(SystemStreamPartition ssp, IncomingMessageEnvelope env) {
        try {
            put(ssp, env);
        } catch (InterruptedException e) {
            LOG.error("Something went wrong while updating Samza queue.");
            e.printStackTrace();
        }
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, String startingOffset) {
        super.register(systemStreamPartition, startingOffset);
        this.amqListeners.add(new ActiveMQListener(this, systemStreamPartition, getAckMode()));
    }

    @Override
    public void start() {
        for (ActiveMQListener amqListener : amqListeners)
            amqListener.run();
    }

    @Override
    public void stop() {
        if (!this.amqListeners.isEmpty())
            for (ActiveMQListener amqListener : amqListeners)
                if (amqListener != null)
                    amqListener.stop();
    }

    public int getAckMode() {
        return ackMode.value;
    }

    public void setAckMode(AmqAckMode ackMode) {
        this.ackMode = ackMode;
    }
}
