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

import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ActiveMQ consumer class
 */
public class ActiveMQConsumer extends BlockingEnvelopeMap {

    private static final String TCP_PROTOCOL = "tcp://";
    private static final String Q_SEPARATOR = ".";
    private String brokerUrl;
    private String queueName;
    private AmqAckMode ackMode;
    private ActiveMQListener amqListener;

    /**
     * Logger for the ActiveMQConsumer
     */
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQConsumer.class);

    /**
     * javax.jms.Session acknowledgement types
     */
    public enum AmqAckMode {
        AUTO_ACKNOWLEDGE(1), CLIENT_ACKNOWLEDGE(2), DUPS_OK_ACKNOWLEDGE(3);

        int value;
        AmqAckMode(int val) {
            value = val;
        }
    }

    public ActiveMQConsumer(String aMode) {
        this.setAckMode(AmqAckMode.valueOf(aMode));
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, String startingOffset) {
        super.register(systemStreamPartition, startingOffset);

        System.out.println("=========================");
        System.out.println("ActiveMQConsumer" + systemStreamPartition);
        String [] brokerUrl = systemStreamPartition.getSystemStream().getStream().split(Q_SEPARATOR);
        if (brokerUrl != null & brokerUrl.length > 1) {
            this.setBrokerUrl(brokerUrl[1]);
            this.setQueueName(brokerUrl[2]);
            amqListener = new ActiveMQListener(getBrokerUrl(), getQueueName(), getAckMode());
        } else {
            LOG.error("ActiveMQ queue format: <server:port>.<queue>");
            LOG.error("Got: " + systemStreamPartition.getSystemStream().getStream());
            throw new SamzaException("ActiveMQ wrong queue format!");
        }
        System.out.println("=========================");


    }

    @Override
    public void start() {
        this.amqListener.run();
    }

    @Override
    public void stop() {
        this.amqListener.stop();
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = TCP_PROTOCOL + queueName;
    }

    public int getAckMode() {
        return ackMode.value;
    }

    public void setAckMode(AmqAckMode ackMode) {
        this.ackMode = ackMode;
    }
}
