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

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;

import static samza.examples.amq.system.ActiveMQConstants.DEFAULT_TRANSAC_SESSION;

/**
 * ActiveMQ factory
 */
public class ActiveMQSystemFactory implements SystemFactory {

    private static final String DEFAULT_JMS_ACK = "AUTO_ACKNOWLEDGE";

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
        return new ActiveMQSystemAdmin();
    }

    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
        return new ActiveMQConsumer(config.get("systems." + systemName + ".jms.ack", DEFAULT_JMS_ACK));
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
        String brokerUrl = config.get("systems." + systemName + ".broker", "");
        String ackMode = config.get("systems." + systemName + ".jms.ack", DEFAULT_JMS_ACK);
        String usr = config.get("systems." + systemName + ".usr", "");
        String psw = config.get("systems." + systemName + ".psw", "");
        boolean tSession = config.getBoolean("systems." + systemName + "transacted_session", DEFAULT_TRANSAC_SESSION);
        return new ActiveMQProducer(brokerUrl, usr, psw, ackMode, tSession);
    }
}
