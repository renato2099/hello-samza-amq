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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ActiveMQListener implements MessageListener {

    private static final String TCP_PROTOCOL = "tcp://";
    private static final String Q_SEPARATOR = "#";
    private static final boolean DEFAULT_TRANSACTED_SESSION = false;

    /**
     * Logger for the ActiveMQConsumer
     */
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQListener.class);

    /**
     * ActiveMQ parameters
     */
    private int ackMode;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private String queueName;
    private SystemStreamPartition ssp;
    private ActiveMQConsumer amqConsumer;


    public ActiveMQListener(ActiveMQConsumer aConsumer, SystemStreamPartition systemStreamPartition, int aMode) {
        this.ssp = systemStreamPartition;
        this.ackMode = aMode;
        this.amqConsumer = aConsumer;

        String brokerUrl;
        String[] brokerQueue = ssp.getSystemStream().getStream().split(Q_SEPARATOR);
        // checking if the broker-queue was passed in the right format
        if (brokerQueue != null & brokerQueue.length == 2) {
            LOG.debug("Broker: " + brokerQueue[0] + "\tQueue: " + brokerQueue[1]);
            brokerUrl = TCP_PROTOCOL + brokerQueue[0];
            this.queueName = brokerQueue[1];
        } else {
            LOG.error("ActiveMQ queue format: <server:port>.<queue>");
            LOG.error("Got: " + ssp.getSystemStream().getStream());
            throw new SamzaException("ActiveMQ wrong queue format!");
        }
        // creating a connection
        try {
            ConnectionFactory amqConFactory = new ActiveMQConnectionFactory(brokerUrl);
            connection = amqConFactory.createConnection();
        } catch (JMSException e) {
            LOG.error("Exception while creating a connection to " + brokerUrl);
            e.printStackTrace();
        }

    }

    public void stop() {
        try {
            this.consumer.close();
            this.session.close();
            this.connection.close();
        } catch (javax.jms.JMSException e) {
            LOG.error("Exception while using the ActiveMQListener:" + e);
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            connection.start();
            session = connection.createSession(DEFAULT_TRANSACTED_SESSION, ackMode);
            Destination destination = session.createQueue(queueName);
            consumer = session.createConsumer(destination);
            consumer.setMessageListener(this);
        } catch (Exception e) {
            LOG.error("Exception while using the ActiveMQListener:" + e);
            e.printStackTrace();
        }
    }

    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {

                TextMessage txtMessage = (TextMessage) message;
                IncomingMessageEnvelope env = new IncomingMessageEnvelope(this.ssp, txtMessage.getJMSMessageID(), txtMessage.getJMSMessageID(), txtMessage);
                this.amqConsumer.putMessage(this.ssp, env);
            }
            //BytesMessage, MapMessage, ObjectMessage, StreamMessage
            else {
                LOG.error("Invalid ActiveMQ message type received.");
                throw new SamzaException("ActiveMQ class not supported: " + message.getClass());
            }
        } catch (JMSException e) {
            LOG.error("Exception while using the ActiveMQListener:" + e);
            e.printStackTrace();
        }
    }
}
