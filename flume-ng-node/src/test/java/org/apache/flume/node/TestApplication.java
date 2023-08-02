/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.node;

import static org.mockito.Mockito.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.Transaction;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.EventProcessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;

public class TestApplication {

  private File baseDir;

  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(baseDir);
  }

  private <T extends LifecycleAware> T mockLifeCycle(Class<T> klass) {

    T lifeCycleAware = mock(klass);

    final AtomicReference<LifecycleState> state =
        new AtomicReference<LifecycleState>();

    state.set(LifecycleState.IDLE);

    when(lifeCycleAware.getLifecycleState()).then(new Answer<LifecycleState>() {
      @Override
      public LifecycleState answer(InvocationOnMock invocation)
          throws Throwable {
        return state.get();
      }
    });

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        state.set(LifecycleState.START);
        return null;
      }
    }).when(lifeCycleAware).start();

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        state.set(LifecycleState.STOP);
        return null;
      }
    }).when(lifeCycleAware).stop();

    return lifeCycleAware;
  }

  @Test
  public void testBasicConfiguration() throws Exception {

    EventBus eventBus = new EventBus("test-event-bus");

    MaterializedConfiguration materializedConfiguration = new
        SimpleMaterializedConfiguration();

    SourceRunner sourceRunner = mockLifeCycle(SourceRunner.class);
    materializedConfiguration.addSourceRunner("test", sourceRunner);

    SinkRunner sinkRunner = mockLifeCycle(SinkRunner.class);
    materializedConfiguration.addSinkRunner("test", sinkRunner);

    Channel channel = mockLifeCycle(Channel.class);
    materializedConfiguration.addChannel("test", channel);


    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getConfiguration()).thenReturn(materializedConfiguration);

    Application application = new Application();
    eventBus.register(application);
    eventBus.post(materializedConfiguration);
    application.start();

    Thread.sleep(1000L);

    verify(sourceRunner).start();
    verify(sinkRunner).start();
    verify(channel).start();

    application.stop();

    Thread.sleep(1000L);

    verify(sourceRunner).stop();
    verify(sinkRunner).stop();
    verify(channel).stop();
  }

  @Test
  public void testFLUME1854() throws Exception {
    File configFile = new File(baseDir, "flume-conf.properties");
    Files.copy(new File(getClass().getClassLoader()
        .getResource("flume-conf.properties").getFile()), configFile);
    Random random = new Random();
    for (int i = 0; i < 3; i++) {
      EventBus eventBus = new EventBus("test-event-bus");
      PollingPropertiesFileConfigurationProvider configurationProvider =
          new PollingPropertiesFileConfigurationProvider("host1",
              configFile, eventBus, 1);
      List<LifecycleAware> components = Lists.newArrayList();
      components.add(configurationProvider);
      Application application = new Application(components);
      eventBus.register(application);
      application.start();
      Thread.sleep(random.nextInt(10000));
      application.stop();
    }
  }

  @Test(timeout = 10000L)
  public void testFLUME2786() throws Exception {
    final String agentName = "test";
    final int interval = 1;
    final long intervalMs = 1000L;

    File configFile = new File(baseDir, "flume-conf.properties");
    Files.copy(new File(getClass().getClassLoader()
        .getResource("flume-conf.properties.2786").getFile()), configFile);
    File mockConfigFile = spy(configFile);
    when(mockConfigFile.lastModified()).then(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(intervalMs);
        return System.currentTimeMillis();
      }
    });

    EventBus eventBus = new EventBus(agentName + "-event-bus");
    PollingPropertiesFileConfigurationProvider configurationProvider =
        new PollingPropertiesFileConfigurationProvider(agentName,
            mockConfigFile, eventBus, interval);
    PollingPropertiesFileConfigurationProvider mockConfigurationProvider =
        spy(configurationProvider);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(intervalMs);
        invocation.callRealMethod();
        return null;
      }
    }).when(mockConfigurationProvider).stop();

    List<LifecycleAware> components = Lists.newArrayList();
    components.add(mockConfigurationProvider);
    Application application = new Application(components);
    eventBus.register(application);
    application.start();
    Thread.sleep(1500L);
    application.stop();
  }

  @Test
  public void testFlumeInit() throws Exception {
    File configFile = new File(baseDir, "flume-conf-init.properties");
    Files.copy(new File(getClass().getClassLoader()
            .getResource("flume-conf-init.properties").getFile()), configFile);
    ConfigurationSource source = new FileConfigurationSource(configFile.toURI());
    List<ConfigurationSource> sourceList = new ArrayList<>();
    sourceList.add(source);
    UriConfigurationProvider configurationProvider =
        new UriConfigurationProvider("host1", sourceList, null, null, 1);
    List<LifecycleAware> components = Lists.newArrayList();
    Application application = new Application(components);
    MaterializedConfiguration configuration = configurationProvider.getConfiguration();
    Assert.assertNotNull("Unable to create configuration", configuration);
    application.handleConfigurationEvent(configuration);
    application.start();
    Map<String, Channel> channels = configuration.getChannels();
    Channel channel = channels.get("processedChannel");
    Assert.assertNotNull("Channel not found", channel);
    Map<String, SourceRunner> sourceRunners = configuration.getSourceRunners();
    Assert.assertNotNull("No source runners", sourceRunners);
    SourceRunner runner = sourceRunners.get("source1");
    Assert.assertNotNull("No source runner", runner);
    EventProcessor processor = (EventProcessor) runner.getSource();
    long[] expected = new long[]{1, 3, 6, 10, 15};
    for (int i = 0; i < 5; ++i) {
      Event event = new SimpleEvent();
      event.setBody(Long.toString(i + 1).getBytes(StandardCharsets.UTF_8));
      processor.processEvent(event);
    }
    Thread.sleep(500);
    for (int i = 0; i < 5; ++i) {
      Event event = getEvent(channel);
      Assert.assertNotNull("No event returned on iteration " + i, event);
      String val = event.getHeaders().get("Total");
      Assert.assertNotNull("No Total in event " + i, val);
      long total = Long.parseLong(val);
      Assert.assertEquals(expected[i], total);
    }
    application.stop();
  }

  private Event getEvent(Channel channel) {
    Transaction transaction = channel.getTransaction();
    Event event = null;

    try {
      transaction.begin();
      event = channel.take();
      transaction.commit();
    } catch (Exception ex) {
      transaction.rollback();
      Assert.fail("Failed to retrieve Flume Event: " + ex.getMessage());
    } finally {
      transaction.close();
    }
    return event;
  }
}
