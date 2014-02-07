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

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Enumeration;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.After;
import org.junit.Before;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;

public abstract class JMSMessageConsumerTestBase {
  static final String USERNAME = "userName";
  static final String PASSWORD = "password";
  static final String DESTINATION_NAME = "destinationName";
  static final String SELECTOR = "selector";
  static final String TEXT = "text";
  static final InitialContext WONT_USE = null;

  Context context;
  JMSMessageConsumer consumer;
  ConnectionFactory connectionFactory;
  String destinationName;
  JMSDestinationType destinationType;
  JMSDestinationLocator destinationLocator;
  String messageSelector;
  int batchSize;
  long pollTimeout;
  JMSMessageConverter converter;
  Optional<String> userName;
  Optional<String> password;

  Connection connection;
  Session session;
  Queue queue;
  Topic topic;
  MessageConsumer messageConsumer;
  TextMessage message;
  Event event;

  @Before
  public void setup() throws Exception {
    beforeSetup();
    connectionFactory = mock(ConnectionFactory.class);
    connection = mock(Connection.class);
    session = mock(Session.class);
    queue = mock(Queue.class);
    topic = mock(Topic.class);
    messageConsumer = mock(MessageConsumer.class);
    message = mock(TextMessage.class);
    when(message.getPropertyNames()).thenReturn(new Enumeration<Object>() {
      @Override
      public boolean hasMoreElements() {
        return false;
      }
      @Override
      public Object nextElement() {
        throw new UnsupportedOperationException();
      }
    });
    when(message.getText()).thenReturn(TEXT);
    when(connectionFactory.createConnection(USERNAME, PASSWORD)).
      thenReturn(connection);
    when(connection.createSession(true, Session.SESSION_TRANSACTED)).
      thenReturn(session);
    when(session.createQueue(destinationName)).thenReturn(queue);
    when(session.createConsumer(any(Destination.class), anyString()))
      .thenReturn(messageConsumer);
    when(messageConsumer.receiveNoWait()).thenReturn(message);
    when(messageConsumer.receive(anyLong())).thenReturn(message);
    destinationName = DESTINATION_NAME;
    destinationType = JMSDestinationType.QUEUE;
    destinationLocator = JMSDestinationLocator.CDI;
    messageSelector = SELECTOR;
    batchSize = 10;
    pollTimeout = 500L;
    context = new Context();
    converter = new DefaultJMSMessageConverter.Builder().build(context);
    event = converter.convert(message).iterator().next();
    userName = Optional.of(USERNAME);
    password = Optional.of(PASSWORD);
    afterSetup();
  }
  void beforeSetup() throws Exception {

  }
  void afterSetup() throws Exception {

  }
  void beforeTearDown() throws Exception {

  }
  void afterTearDown() throws Exception {

  }
  void assertBodyIsExpected(List<Event> events) {
    for(Event event : events) {
      assertEquals(TEXT, new String(event.getBody(), Charsets.UTF_8));
    }
  }

  JMSMessageConsumer create() {
    return new JMSMessageConsumer(WONT_USE, connectionFactory, destinationName,
        destinationLocator, destinationType, messageSelector, batchSize,
        pollTimeout, converter, userName, password);
  }
  @After
  public void tearDown() throws Exception {
    beforeTearDown();
    if(consumer != null) {
      consumer.close();
    }
    afterTearDown();
  }
}
