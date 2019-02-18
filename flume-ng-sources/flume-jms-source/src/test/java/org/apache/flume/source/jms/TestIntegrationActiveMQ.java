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
import java.util.Arrays;
import java.util.Collection;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestIntegrationActiveMQ {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestIntegrationActiveMQ.class);

  private static final String INITIAL_CONTEXT_FACTORY =
      "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
  public static final String BROKER_BIND_URL = "tcp://localhost:61516";
  private static final String DESTINATION_NAME = "test";
  // specific for dynamic queues on ActiveMq
  public static final String JNDI_PREFIX = "dynamicQueues/";

  private enum TestMode {
    WITH_AUTHENTICATION,
    WITHOUT_AUTHENTICATION
  }

  private File baseDir;
  private File tmpDir;
  private File dataDir;

  private BrokerService broker;
  private Context context;
  private JMSSource source;
  private List<Event> events;

  private final String jmsUserName;
  private final String jmsPassword;

  public TestIntegrationActiveMQ(TestMode testMode) {
    LOGGER.info("Testing with test mode {}", testMode);

    switch (testMode) {
      case WITH_AUTHENTICATION:
        jmsUserName = "user";
        jmsPassword = "pass";
        break;
      case WITHOUT_AUTHENTICATION:
        jmsUserName = null;
        jmsPassword = null;
        break;
      default:
        throw new IllegalArgumentException("Unhandled test mode: " + testMode);
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
      {TestMode.WITH_AUTHENTICATION},
      {TestMode.WITHOUT_AUTHENTICATION}
    });
  }

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
    tmpDir = new File(baseDir, "tmp");
    dataDir = new File(baseDir, "data");
    Assert.assertTrue(tmpDir.mkdir());

    broker = new BrokerService();
    broker.addConnector(BROKER_BIND_URL);
    broker.setTmpDataDirectory(tmpDir);
    broker.setDataDirectoryFile(dataDir);

    context = new Context();
    context.put(JMSSourceConfiguration.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY);
    context.put(JMSSourceConfiguration.PROVIDER_URL, BROKER_BIND_URL);
    context.put(JMSSourceConfiguration.DESTINATION_NAME, DESTINATION_NAME);

    if (jmsUserName != null) {
      File passwordFile = new File(baseDir, "password");
      Files.write(jmsPassword.getBytes(Charsets.UTF_8), passwordFile);

      AuthenticationUser jmsUser = new AuthenticationUser(jmsUserName, jmsPassword, "");
      List<AuthenticationUser> users = Collections.singletonList(jmsUser);
      SimpleAuthenticationPlugin authentication = new SimpleAuthenticationPlugin(users);
      broker.setPlugins(new BrokerPlugin[]{authentication});

      context.put(JMSSourceConfiguration.USERNAME, jmsUserName);
      context.put(JMSSourceConfiguration.PASSWORD_FILE, passwordFile.getAbsolutePath());
    }

    broker.start();

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
    if (source != null) {
      source.stop();
    }
    if (broker != null) {
      broker.stop();
    }
    FileUtils.deleteDirectory(baseDir);
  }

  private void putQueue(List<String> events) throws Exception {
    ConnectionFactory factory = new ActiveMQConnectionFactory(jmsUserName, jmsPassword,
        BROKER_BIND_URL);
    Connection connection = factory.createConnection();
    connection.start();

    Session session = connection.createSession(true,
        Session.AUTO_ACKNOWLEDGE);
    Destination destination = session.createQueue(DESTINATION_NAME);
    MessageProducer producer = session.createProducer(destination);

    for (String event : events) {
      TextMessage message = session.createTextMessage();
      message.setText(event);
      producer.send(message);
    }
    session.commit();
    session.close();
    connection.close();
  }

  private void putTopic(List<String> events) throws Exception {
    ConnectionFactory factory = new ActiveMQConnectionFactory(jmsUserName, jmsPassword,
        BROKER_BIND_URL);
    Connection connection = factory.createConnection();
    connection.start();

    Session session = connection.createSession(true,
        Session.AUTO_ACKNOWLEDGE);
    Destination destination = session.createTopic(DESTINATION_NAME);
    MessageProducer producer = session.createProducer(destination);

    for (String event : events) {
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
    for (Event event : events) {
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
    for (Event event : events) {
      actual.add(new String(event.getBody(), Charsets.UTF_8));
    }
    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDurableSubscription() throws Exception {
    context.put(JMSSourceConfiguration.DESTINATION_TYPE,
        JMSSourceConfiguration.DESTINATION_TYPE_TOPIC);
    context.put(JMSSourceConfiguration.CLIENT_ID, "FLUME");
    context.put(JMSSourceConfiguration.DURABLE_SUBSCRIPTION_NAME, "SOURCE1");
    context.put(JMSSourceConfiguration.CREATE_DURABLE_SUBSCRIPTION, "true");
    context.put(JMSSourceConfiguration.BATCH_SIZE, "10");
    source.configure(context);
    source.start();
    Thread.sleep(5000L);
    List<String> expected = Lists.newArrayList();
    List<String> input = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      input.add("before " + String.valueOf(i));
    }
    expected.addAll(input);
    putTopic(input);

    Thread.sleep(500L);
    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(Status.BACKOFF, source.process());
    source.stop();
    Thread.sleep(500L);
    input = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      input.add("during " + String.valueOf(i));
    }
    expected.addAll(input);
    putTopic(input);
    source.start();
    Thread.sleep(500L);
    input = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      input.add("after " + String.valueOf(i));
    }
    expected.addAll(input);
    putTopic(input);

    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(Status.BACKOFF, source.process());
    Assert.assertEquals(expected.size(), events.size());
    List<String> actual = Lists.newArrayList();
    for (Event event : events) {
      actual.add(new String(event.getBody(), Charsets.UTF_8));
    }
    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }
}