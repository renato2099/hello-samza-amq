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

/**
 * Class containing constants for ActiveMQ consumer and producer.
 */
public class ActiveMQConstants {
    /**
     * javax.jms.Session acknowledgement types
     */
    public static enum AmqAckMode {
        AUTO_ACKNOWLEDGE(1), CLIENT_ACKNOWLEDGE(2), DUPS_OK_ACKNOWLEDGE(3);

        int value;

        AmqAckMode(int val) {
            value = val;
        }
    }

    /**
     * Default connection protocol.
     */
    public static final String DEFAULT_TCP_PROTOCOL = "tcp://";

    /**
     * Default character separator for specifying queues.
     */
    public static final String DEFAULT_Q_SEPARATOR = "#";

    /**
     * Default transacted session.
     */
    public static final boolean DEFAULT_TRANSAC_SESSION = false;
}
