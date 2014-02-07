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
package org.apache.flume.source.jms;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Assert;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestIntegrationActiveMQ {

  private final static String INITIAL_CONTEXT_FACTORY = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
  public static final String BROKER_BIND_URL = "tcp://localhost:61516";
  private final static  String DESTINATION_NAME = "test";
  private static final String USERNAME = "user";
  private static final String PASSWORD = "pass";
  // specific for dynamic queues on ActiveMq
  public static final String JNDI_PREFIX = "dynamicQueues/";

  private File baseDir;
  private File tmpDir;
  private File dataDir;
  private File passwordFile;

  private BrokerService broker;
  private Context context;
  private JMSSource source;
  private List<Event> events;


  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
    tmpDir = new File(baseDir, "tmp");
    dataDir = new File(baseDir, "data");
    Assert.assertTrue(tmpDir.mkdir());
    passwordFile = new File(baseDir, "password");
    Files.write(PASSWORD.getBytes(Charsets.UTF_8), passwordFile);

    broker = new BrokerService();

    broker.addConnector(BROKER_BIND_URL);
    broker.setTmpDataDirectory(tmpDir);
    broker.setDataDirectoryFile(dataDir);
    List<AuthenticationUser> users = Lists.newArrayList();
    users.add(new AuthenticationUser(USERNAME, PASSWORD, ""));
    SimpleAuthenticationPlugin authentication = new SimpleAuthenticationPlugin(users);
    broker.setPlugins(new BrokerPlugin[]{authentication});
    broker.start();

    context = new Context();
    context.put(JMSSourceConfiguration.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY);
    context.put(JMSSourceConfiguration.PROVIDER_URL, BROKER_BIND_URL);
    context.put(JMSSourceConfiguration.DESTINATION_NAME, DESTINATION_NAME);
    context.put(JMSSourceConfiguration.USERNAME, USERNAME);
    context.put(JMSSourceConfiguration.PASSWORD_FILE, passwordFile.getAbsolutePath());

    events = Lists.newArrayList();
    source = new JMSSource();
    source.setName("JMSSource-" + UUID.randomUUID());
    ChannelProcessor channelProcessor = mock(ChannelProcessor.class);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        events.addAll((List<Event>)invocation.getArguments()[0]);
        return null;
      }
    }).when(channelProcessor).processEventBatch(any(List.class));
    source.setChannelProcessor(channelProcessor);


  }
  @After
  public void tearDown() throws Exception {
    if(source != null) {
      source.stop();
    }
    if(broker != null) {
      broker.stop();
    }
    FileUtils.deleteDirectory(baseDir);
  }

  private void putQueue(List<String> events) throws Exception {
    ConnectionFactory factory = new ActiveMQConnectionFactory(USERNAME,
        PASSWORD, BROKER_BIND_URL);
    Connection connection = factory.createConnection();
    connection.start();

    Session session = connection.createSession(true,
        Session.AUTO_ACKNOWLEDGE);
    Destination destination = session.createQueue(DESTINATION_NAME);
    MessageProducer producer = session.createProducer(destination);


    for(String event : events) {
      TextMessage message = session.createTextMessage();
      message.setText(event);
      producer.send(message);
    }
    session.commit();
    session.close();
    connection.close();
  }

  private void putTopic(List<String> events) throws Exception {
    ConnectionFactory factory = new ActiveMQConnectionFactory(USERNAME,
        PASSWORD, BROKER_BIND_URL);
    Connection connection = factory.createConnection();
    connection.start();

    Session session = connection.createSession(true,
        Session.AUTO_ACKNOWLEDGE);
    Destination destination = session.createTopic(DESTINATION_NAME);
    MessageProducer producer = session.createProducer(destination);


    for(String event : events) {
      TextMessage message = session.createTextMessage();
      message.setText(event);
      producer.send(message);
    }
    session.commit();
    session.close();
    connection.close();
  }

  @Test
  public void testQueueLocatedWithJndi() throws Exception {
    context.put(JMSSourceConfiguration.DESTINATION_NAME,
            JNDI_PREFIX + DESTINATION_NAME);
    context.put(JMSSourceConfiguration.DESTINATION_LOCATOR,
            JMSDestinationLocator.JNDI.name());
    testQueue();
  }

  @Test
  public void testQueue() throws Exception {
    context.put(JMSSourceConfiguration.DESTINATION_TYPE,
        JMSSourceConfiguration.DESTINATION_TYPE_QUEUE);
    source.configure(context);
    source.start();
    Thread.sleep(500L);

    List<String> expected = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      expected.add(String.valueOf(i));
    }
    putQueue(expected);

    Thread.sleep(500L);

    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(Status.BACKOFF, source.process());
    Assert.assertEquals(expected.size(), events.size());
    List<String> actual = Lists.newArrayList();
    for(Event event : events) {
      actual.add(new String(event.getBody(), Charsets.UTF_8));
    }
    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }
  @Test
  public void testTopic() throws Exception {
    context.put(JMSSourceConfiguration.DESTINATION_TYPE,
        JMSSourceConfiguration.DESTINATION_TYPE_TOPIC);
    source.configure(context);
    source.start();
    Thread.sleep(500L);

    List<String> expected = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      expected.add(String.valueOf(i));
    }
    putTopic(expected);

    Thread.sleep(500L);

    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(Status.BACKOFF, source.process());
    Assert.assertEquals(expected.size(), events.size());
    List<String> actual = Lists.newArrayList();
    for(Event event : events) {
      actual.add(new String(event.getBody(), Charsets.UTF_8));
    }
    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }
}