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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ActiveMQListener implements MessageListener {

    /**
     * Logger for the ActiveMQConsumer
     */
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQListener.class);

    /**
     * ActiveMQ parameters
     */
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private String queueName;
    private int ackMode;
    private boolean DEFAULT_TRANSACTED_SESSION = false;

    public ActiveMQListener(String brokerUrl, String qName, int aMode) {
        ackMode = aMode;
        try {
            ConnectionFactory amqConFactory = new ActiveMQConnectionFactory(brokerUrl);
            connection = amqConFactory.createConnection();
        } catch (JMSException e) {
            LOG.error("Exception while creating a connection to " + brokerUrl);
            e.printStackTrace();
        }
        queueName = qName;
    }

    public void stop() {
        try{
            this.consumer.close();
            this.session.close();
            this.connection.close();
        } catch (javax.jms.JMSException e) {
            LOG.error("Exception while using the ActiveMQListener:" + e);
            e.printStackTrace();
        }
    }

    public void run()
    {
        try
        {
            connection.start();
            session = connection.createSession(DEFAULT_TRANSACTED_SESSION, ackMode);
            Destination destination = session.createQueue(queueName);
            consumer = session.createConsumer(destination);
            consumer.setMessageListener(this);
        }
        catch (Exception e)
        {
            LOG.error("Exception while using the ActiveMQListener:" + e);
            e.printStackTrace();
        }
    }

    public void onMessage(Message message)
    {
        try
        {
            if (message instanceof TextMessage)
            {
                TextMessage txtMessage = (TextMessage)message;
                System.out.println("Message received: " + txtMessage.getText());
                System.out.println("Message id: " + txtMessage.getJMSMessageID());
            }
            //BytesMessage, MapMessage, ObjectMessage, StreamMessage
            else
            {
                System.out.println("Invalid message received.");
            }
        }
        catch (JMSException e)
        {
            LOG.error("Exception while using the ActiveMQListener:" + e);
            e.printStackTrace();
        }
    }
}
