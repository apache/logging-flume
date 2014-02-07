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
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestJMSSource extends JMSMessageConsumerTestBase {

  private JMSSource source;
  private Context context;
  private InitialContext initialContext;
  private ChannelProcessor channelProcessor;
  private List<Event> events;
  private JMSMessageConsumerFactory consumerFactory;
  private InitialContextFactory contextFactory;
  private File baseDir;
  private File passwordFile;
  @SuppressWarnings("unchecked")
  @Override
  void afterSetup() throws Exception {
    baseDir = Files.createTempDir();
    passwordFile = new File(baseDir, "password");
    Assert.assertTrue(passwordFile.createNewFile());
    initialContext = mock(InitialContext.class);
    channelProcessor = mock(ChannelProcessor.class);
    events = Lists.newArrayList();
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        events.addAll((List<Event>)invocation.getArguments()[0]);
        return null;
      }
    }).when(channelProcessor).processEventBatch(any(List.class));
    consumerFactory = mock(JMSMessageConsumerFactory.class);
    consumer = spy(create());
    when(consumerFactory.create(any(InitialContext.class), any(ConnectionFactory.class), anyString(),
        any(JMSDestinationType.class), any(JMSDestinationLocator.class), anyString(), anyInt(), anyLong(),
        any(JMSMessageConverter.class), any(Optional.class),
        any(Optional.class))).thenReturn(consumer);
    when(initialContext.lookup(anyString())).thenReturn(connectionFactory);
    contextFactory = mock(InitialContextFactory.class);
    when(contextFactory.create(any(Properties.class))).thenReturn(initialContext);
    source = new JMSSource(consumerFactory, contextFactory);
    source.setName("JMSSource-" + UUID.randomUUID());
    source.setChannelProcessor(channelProcessor);
    context = new Context();
    context.put(JMSSourceConfiguration.BATCH_SIZE, String.valueOf(batchSize));
    context.put(JMSSourceConfiguration.DESTINATION_NAME, "INBOUND");
    context.put(JMSSourceConfiguration.DESTINATION_TYPE,
        JMSSourceConfiguration.DESTINATION_TYPE_QUEUE);
    context.put(JMSSourceConfiguration.PROVIDER_URL, "dummy:1414");
    context.put(JMSSourceConfiguration.INITIAL_CONTEXT_FACTORY, "ldap://dummy:389");
  }
  @Override
  void afterTearDown() throws Exception {
    FileUtils.deleteDirectory(baseDir);
  }
  @Test
  public void testStop() throws Exception {
    source.configure(context);
    source.start();
    source.stop();
    verify(consumer).close();
  }
  @Test(expected = IllegalArgumentException.class)
  public void testConfigureWithoutInitialContextFactory() throws Exception {
    context.put(JMSSourceConfiguration.INITIAL_CONTEXT_FACTORY, "");
    source.configure(context);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testConfigureWithoutProviderURL() throws Exception {
    context.put(JMSSourceConfiguration.PROVIDER_URL, "");
    source.configure(context);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testConfigureWithoutDestinationName() throws Exception {
    context.put(JMSSourceConfiguration.DESTINATION_NAME, "");
    source.configure(context);
  }
  @Test(expected = FlumeException.class)
  public void testConfigureWithBadDestinationType() throws Exception {
    context.put(JMSSourceConfiguration.DESTINATION_TYPE, "DUMMY");
    source.configure(context);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testConfigureWithEmptyDestinationType() throws Exception {
    context.put(JMSSourceConfiguration.DESTINATION_TYPE, "");
    source.configure(context);
  }
  @SuppressWarnings("unchecked")
  @Test
  public void testStartConsumerCreateThrowsException() throws Exception {
    when(consumerFactory.create(any(InitialContext.class), any(ConnectionFactory.class), anyString(),
        any(JMSDestinationType.class), any(JMSDestinationLocator.class), anyString(), anyInt(), anyLong(),
        any(JMSMessageConverter.class), any(Optional.class),
        any(Optional.class))).thenThrow(new RuntimeException());
    source.configure(context);
    source.start();
    try {
      source.process();
      Assert.fail();
    } catch (FlumeException expected) {

    }
  }
  @Test(expected = FlumeException.class)
  public void testConfigureWithContextLookupThrowsException() throws Exception {
    when(initialContext.lookup(anyString())).thenThrow(new NamingException());
    source.configure(context);
  }
  @Test(expected = FlumeException.class)
  public void testConfigureWithContextCreateThrowsException() throws Exception {
    when(contextFactory.create(any(Properties.class)))
      .thenThrow(new NamingException());
    source.configure(context);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testConfigureWithInvalidBatchSize() throws Exception {
    context.put(JMSSourceConfiguration.BATCH_SIZE, "0");
    source.configure(context);
  }
  @Test(expected = FlumeException.class)
  public void testConfigureWithInvalidPasswordFile() throws Exception {
    context.put(JMSSourceConfiguration.PASSWORD_FILE,
        "/dev/does/not/exist/nor/will/ever/exist");
    source.configure(context);
  }
  @Test
  public void testConfigureWithUserNameButNoPasswordFile() throws Exception {
    context.put(JMSSourceConfiguration.USERNAME, "dummy");
    source.configure(context);
    source.start();
    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(batchSize, events.size());
    assertBodyIsExpected(events);
  }
  @Test
  public void testConfigureWithUserNameAndPasswordFile() throws Exception {
    context.put(JMSSourceConfiguration.USERNAME, "dummy");
    context.put(JMSSourceConfiguration.PASSWORD_FILE,
        passwordFile.getAbsolutePath());
    source.configure(context);
    source.start();
    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(batchSize, events.size());
    assertBodyIsExpected(events);
  }
  @Test(expected = FlumeException.class)
  public void testConfigureWithInvalidConverterClass() throws Exception {
    context.put(JMSSourceConfiguration.CONVERTER_TYPE, "not a valid classname");
    source.configure(context);
  }
  @Test
  public void testProcessNoStart() throws Exception {
    try {
      source.process();
      Assert.fail();
    } catch (EventDeliveryException expected) {

    }
  }
  @Test
  public void testNonDefaultConverter() throws Exception {
    // tests that a classname can be specified
    context.put(JMSSourceConfiguration.CONVERTER_TYPE,
        DefaultJMSMessageConverter.Builder.class.getName());
    source.configure(context);
    source.start();
    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(batchSize, events.size());
    assertBodyIsExpected(events);
    verify(consumer).commit();
  }
  public static class NonBuilderNonConfigurableConverter
  implements JMSMessageConverter {
    @Override
    public List<Event> convert(Message message) throws JMSException {
      throw new UnsupportedOperationException();
    }
  }
  public static class NonBuilderConfigurableConverter
  implements JMSMessageConverter, Configurable {
    @Override
    public List<Event> convert(Message message) throws JMSException {
      throw new UnsupportedOperationException();
    }
    @Override
    public void configure(Context context) {

    }
  }
  @Test
  public void testNonBuilderConfigurableConverter() throws Exception {
    // tests that a non builder by configurable converter works
    context.put(JMSSourceConfiguration.CONVERTER_TYPE,
        NonBuilderConfigurableConverter.class.getName());
    source.configure(context);
    source.start();
    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(batchSize, events.size());
    assertBodyIsExpected(events);
    verify(consumer).commit();
  }
  @Test
  public void testNonBuilderNonConfigurableConverter() throws Exception {
    // tests that a non builder non configurable converter
    context.put(JMSSourceConfiguration.CONVERTER_TYPE,
        NonBuilderNonConfigurableConverter.class.getName());
    source.configure(context);
    source.start();
    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(batchSize, events.size());
    assertBodyIsExpected(events);
    verify(consumer).commit();
  }
  @Test
  public void testProcessFullBatch() throws Exception {
    source.configure(context);
    source.start();
    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(batchSize, events.size());
    assertBodyIsExpected(events);
    verify(consumer).commit();
  }
  @Test
  public void testProcessNoEvents() throws Exception {
    when(messageConsumer.receive(anyLong())).thenReturn(null);
    source.configure(context);
    source.start();
    Assert.assertEquals(Status.BACKOFF, source.process());
    Assert.assertEquals(0, events.size());
    verify(consumer).commit();
  }
  @Test
  public void testProcessPartialBatch() throws Exception {
    when(messageConsumer.receiveNoWait()).thenReturn(message, (Message)null);
    source.configure(context);
    source.start();
    Assert.assertEquals(Status.READY, source.process());
    Assert.assertEquals(2, events.size());
    assertBodyIsExpected(events);
    verify(consumer).commit();
  }
  @SuppressWarnings("unchecked")
  @Test
  public void testProcessChannelProcessorThrowsChannelException() throws Exception {
    doThrow(new ChannelException("dummy"))
      .when(channelProcessor).processEventBatch(any(List.class));
    source.configure(context);
    source.start();
    Assert.assertEquals(Status.BACKOFF, source.process());
    verify(consumer).rollback();
  }
  @SuppressWarnings("unchecked")
  @Test
  public void testProcessChannelProcessorThrowsError() throws Exception {
    doThrow(new Error())
      .when(channelProcessor).processEventBatch(any(List.class));
    source.configure(context);
    source.start();
    try {
      source.process();
      Assert.fail();
    } catch (Error ignores) {

    }
    verify(consumer).rollback();
  }
  @Test
  public void testProcessReconnect() throws Exception {
    source.configure(context);
    source.start();
    when(consumer.take()).thenThrow(new JMSException("dummy"));
    int attempts = JMSSourceConfiguration.ERROR_THRESHOLD_DEFAULT;
    for (int i = 0; i < attempts; i++) {
      Assert.assertEquals(Status.BACKOFF, source.process());
    }
    Assert.assertEquals(Status.BACKOFF, source.process());
    verify(consumer, times(attempts + 1)).rollback();
    verify(consumer, times(1)).close();
  }
}