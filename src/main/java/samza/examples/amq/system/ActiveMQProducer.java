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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

import static samza.examples.amq.system.ActiveMQConstants.AmqAckMode;
import static samza.examples.amq.system.ActiveMQConstants.DEFAULT_TCP_PROTOCOL;

/**
 * ActiveMQ producer.
 */
public class ActiveMQProducer implements SystemProducer {
    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQProducer.class);

    private Connection connection;
    private Session session;
    /**
     * Enforcing a 1:1 mapping between queues and msg producers.
     */
    private Map<String, MessageProducer> msgProducers;
    private boolean tranSession;
    private AmqAckMode ackMode;

    public ActiveMQProducer(String brokUrl, String user, String psw, String aMode, boolean tSession) {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(DEFAULT_TCP_PROTOCOL + brokUrl);
        this.msgProducers = new HashMap<String, MessageProducer>();
        this.tranSession = tSession;
        this.ackMode = AmqAckMode.valueOf(aMode);
        try {
            this.connection = factory.createConnection(user, psw);
        } catch (JMSException e) {
            LOG.error("Error while connecting to " + brokUrl);
            e.printStackTrace();
        }
    }

    @Override
    public void start() {
        // Nothing to do
    }

    @Override
    public void stop() {
        try {
            if (this.session != null)
                this.session.close();
            if (this.connection != null)
                this.connection.stop();
        } catch (JMSException e) {
            LOG.error("Error while stopping the session/connection.");
            e.printStackTrace();
        }
    }

    @Override
    public void register(String stream) {
        try {
            this.connection.start();
            this.session = connection.createSession(tranSession, ackMode.value);
        } catch (JMSException e) {
            LOG.error(String.format("Error while registering a stream %s.", stream));
            e.printStackTrace();
        }
    }

    @Override
    public void send(String source, OutgoingMessageEnvelope outgoingMessageEnvelope) {
        String queueName = outgoingMessageEnvelope.getSystemStream().getStream();
        String strMsg = new String((byte[]) outgoingMessageEnvelope.getMessage());
        try {
            if (this.msgProducers.get(queueName) == null) {
                Destination d = session.createQueue(queueName);
                this.msgProducers.put(queueName, session.createProducer(d));
            }
            MessageProducer producer = this.msgProducers.get(queueName);
            // TODO add support for message types
            producer.send(session.createTextMessage(strMsg));
        } catch (JMSException e) {
            LOG.error(String.format("Error while sending message %s.", strMsg));
            e.printStackTrace();
        }
    }

    @Override
    public void flush(String s) {
        try {
            if (tranSession)
                this.session.commit();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

}
