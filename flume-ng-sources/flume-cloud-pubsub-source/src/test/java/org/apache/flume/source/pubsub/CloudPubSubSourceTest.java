/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.pubsub;

import static org.apache.flume.source.pubsub.CloudPubSubSourceContstants.SERVICE_ACCOUNT_KEY_PATH;
import static org.apache.flume.source.pubsub.CloudPubSubSourceContstants.SUBSCRIPTION;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.Pubsub.Projects;
import com.google.api.services.pubsub.Pubsub.Projects.Subscriptions;
import com.google.api.services.pubsub.Pubsub.Projects.Subscriptions.Acknowledge;
import com.google.api.services.pubsub.Pubsub.Projects.Subscriptions.Pull;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.Empty;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;

import junit.framework.Assert;

@RunWith(MockitoJUnitRunner.class)
public class CloudPubSubSourceTest {
  private static final String MOCK_SUBSCRIPTION_NAME = "projects/test/subscriptions/test";
  private static final String MOCK_ACCCESS_KEY_FILE_PATH = "mock-access-key.json";
  @Mock
  private Pubsub mockPubSub;
  @Mock
  private Projects mockProjects;
  @Mock
  private Subscriptions mockSubscriptions;
  @Mock
  private Pull mockPull;
  @Mock
  private Acknowledge mockAcknowledge;
  @Mock
  private ChannelProcessor mockChannelProcessor;
  private PullResponse mockPullResponse;
  private List<Event> processedEvents;
  private List<String> ackIds;

  public void setupMock(List<ReceivedMessage> mockReceivedMessages) throws IOException {
    processedEvents = new ArrayList<>();
    ackIds = new ArrayList<>();
    mockPullResponse = new PullResponse().setReceivedMessages(mockReceivedMessages);
    when(mockPubSub.projects()).thenReturn(mockProjects);
    when(mockProjects.subscriptions()).thenReturn(mockSubscriptions);
    when(mockSubscriptions.pull(eq(MOCK_SUBSCRIPTION_NAME), isA(PullRequest.class)))
        .thenReturn(mockPull);
    when(mockAcknowledge.execute()).thenReturn(new Empty());
    when(mockPull.execute()).thenReturn(mockPullResponse);

    // store Flume events that are processed by source
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        processedEvents.addAll((List<Event>) invocation.getArguments()[0]);
        return null;
      }
    }).when(mockChannelProcessor).processEventBatch(any(List.class));

    // store AckID
    when(mockSubscriptions.acknowledge(eq(MOCK_SUBSCRIPTION_NAME), isA(AcknowledgeRequest.class)))
        .then(new Answer<Acknowledge>() {
          @Override
          public Acknowledge answer(InvocationOnMock invocation) throws Throwable {
            AcknowledgeRequest acknowledgeRequest 
                = (AcknowledgeRequest) invocation.getArguments()[1];
            ackIds.addAll(acknowledgeRequest.getAckIds());
            return mockAcknowledge;
          }
        });
  }

  @After
  public void clean() {
    this.mockPullResponse = null;
    this.processedEvents = null;
  }

  @Test
  public void testSubscribeOneEvent() throws Exception {
    setupMock(Arrays.asList(new ReceivedMessage().setAckId("ack")
        .setMessage(new PubsubMessage().encodeData("mock_data".getBytes()))));

    Context context = new Context();
    context.put(SUBSCRIPTION, MOCK_SUBSCRIPTION_NAME);
    context.put(SERVICE_ACCOUNT_KEY_PATH, MOCK_ACCCESS_KEY_FILE_PATH);

    CloudPubSubSource source = new CloudPubSubSource();
    try {
      source.configure(context);
    } catch (IllegalStateException e) {
      // throw IllegalStateException because of a file is not exist.
      Assert.assertEquals("Failed to create pubsub client", e.getMessage());
    }
    source.setMockPubSub(mockPubSub);
    source.setChannelProcessor(mockChannelProcessor);
    source.start();
    Status status = source.process();
    source.stop();
    Assert.assertEquals(Status.READY, status);
    Assert.assertEquals(1, processedEvents.size());
    Assert.assertEquals(1, ackIds.size());
    Assert.assertEquals("mock_data", new String(processedEvents.get(0).getBody()));
    Assert.assertEquals("ack", new String(ackIds.get(0)));
  }

  @Test
  public void testMessageIsEmpty() throws Exception {
    setupMock(Arrays.asList(new ReceivedMessage().setAckId("ack")));

    Context context = new Context();
    context.put(SUBSCRIPTION, MOCK_SUBSCRIPTION_NAME);
    context.put(SERVICE_ACCOUNT_KEY_PATH, MOCK_ACCCESS_KEY_FILE_PATH);

    CloudPubSubSource source = new CloudPubSubSource();
    try {
      source.configure(context);
    } catch (IllegalStateException e) {
      // throw IllegalStateException because of a file is not exist.
      Assert.assertEquals("Failed to create pubsub client", e.getMessage());
    }
    source.setMockPubSub(mockPubSub);
    source.setChannelProcessor(mockChannelProcessor);
    source.start();
    Status status = source.process();
    source.stop();
    Assert.assertEquals(Status.READY, status);
    Assert.assertEquals(0, processedEvents.size());
    Assert.assertEquals(0, ackIds.size());
  }

  @Test
  public void testAckIdisEmpty() throws Exception {
    setupMock(Arrays.asList(new ReceivedMessage()
        .setMessage(new PubsubMessage().encodeData("mock_data".getBytes()))));

    Context context = new Context();
    context.put(SUBSCRIPTION, MOCK_SUBSCRIPTION_NAME);
    context.put(SERVICE_ACCOUNT_KEY_PATH, MOCK_ACCCESS_KEY_FILE_PATH);

    CloudPubSubSource source = new CloudPubSubSource();
    try {
      source.configure(context);
    } catch (IllegalStateException e) {
      // throw IllegalStateException because of a file is not exist.
      Assert.assertEquals("Failed to create pubsub client", e.getMessage());
    }
    source.setMockPubSub(mockPubSub);
    source.setChannelProcessor(mockChannelProcessor);
    source.start();
    Status status = source.process();
    source.stop();
    Assert.assertEquals(Status.READY, status);
    Assert.assertEquals(0, processedEvents.size());
    Assert.assertEquals(0, ackIds.size());
  }

}
