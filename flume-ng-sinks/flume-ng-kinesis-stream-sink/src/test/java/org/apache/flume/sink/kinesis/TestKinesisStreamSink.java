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
package org.apache.flume.sink.kinesis;

import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.CONFIG_KEY_AWS_REGION;
import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.CONFIG_KEY_STREAM_NAME;
import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.HEADER_KEY_PARTITION_KEY;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

import junit.framework.Assert;

@RunWith(MockitoJUnitRunner.class)
public class TestKinesisStreamSink {
  private static final String MOCK_STREAM_NAME = "mock_stream";
  private static final String MOCK_REGION = "us-east-1";
  @Mock
  private AmazonKinesisClient mockAmazonKinesisClient;
  @Mock
  private Channel mockChannel;
  @Mock
  private Transaction mockTransaction;
  private List<PutRecordRequest> mockPutRecordRequests;

  @Before
  public void setup() {
    this.mockPutRecordRequests = new ArrayList<>();
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        mockPutRecordRequests.add((PutRecordRequest) invocation.getArguments()[0]);
        return null;
      }
    }).when(mockAmazonKinesisClient).putRecord(isA(PutRecordRequest.class));
    when(mockChannel.getTransaction()).thenReturn(mockTransaction);
  }

  @After
  public void clean() {
    this.mockPutRecordRequests = null;
  }

  @Test
  public void testMessageWithPartitionKey() throws EventDeliveryException {
    Map<String, String> headers = new HashMap<>();
    headers.put(HEADER_KEY_PARTITION_KEY, "mock_partition_key");
    when(mockChannel.take()).thenReturn(EventBuilder.withBody("mock_data".getBytes(), headers));

    Context context = new Context();
    context.put(CONFIG_KEY_STREAM_NAME, MOCK_STREAM_NAME);
    context.put(CONFIG_KEY_AWS_REGION, MOCK_REGION);
    KinesisStreamSink kinesisStreamSink = new KinesisStreamSink();
    kinesisStreamSink.configure(context);
    kinesisStreamSink.setMockClient(mockAmazonKinesisClient);
    kinesisStreamSink.setChannel(mockChannel);
    kinesisStreamSink.start();
    Status status = kinesisStreamSink.process();
    kinesisStreamSink.stop();
    Assert.assertEquals(Status.READY, status);
    Assert.assertEquals(1, mockPutRecordRequests.size());
    Assert.assertEquals(MOCK_STREAM_NAME, mockPutRecordRequests.get(0).getStreamName());
    Assert.assertEquals("mock_partition_key", mockPutRecordRequests.get(0).getPartitionKey());
    Assert.assertEquals(ByteBuffer.wrap("mock_data".getBytes()),
        mockPutRecordRequests.get(0).getData());
  }

  @Test
  public void testMessageWithoutPartitionKey() throws EventDeliveryException {
    when(mockChannel.take()).thenReturn(EventBuilder.withBody("mock_data".getBytes()));

    Context context = new Context();
    context.put(CONFIG_KEY_STREAM_NAME, MOCK_STREAM_NAME);
    context.put(CONFIG_KEY_AWS_REGION, MOCK_REGION);
    KinesisStreamSink kinesisStreamSink = new KinesisStreamSink();
    kinesisStreamSink.configure(context);
    kinesisStreamSink.setMockClient(mockAmazonKinesisClient);
    kinesisStreamSink.setChannel(mockChannel);
    kinesisStreamSink.start();
    Status status = kinesisStreamSink.process();
    kinesisStreamSink.stop();

    Assert.assertEquals(Status.READY, status);
    Assert.assertEquals(1, mockPutRecordRequests.size());
    Assert.assertEquals(MOCK_STREAM_NAME, mockPutRecordRequests.get(0).getStreamName());
    // source set uuid(version 4) as partition key
    Assert.assertEquals(36, mockPutRecordRequests.get(0).getPartitionKey().length());
    Assert.assertEquals('4', mockPutRecordRequests.get(0).getPartitionKey().charAt(14));
    Assert.assertEquals(ByteBuffer.wrap("mock_data".getBytes()),
        mockPutRecordRequests.get(0).getData());
  }

  @Test
  public void testEventIsEmpty() throws EventDeliveryException {
    when(mockChannel.take()).thenReturn(null);

    Context context = new Context();
    context.put(CONFIG_KEY_STREAM_NAME, MOCK_STREAM_NAME);
    context.put(CONFIG_KEY_AWS_REGION, MOCK_REGION);
    KinesisStreamSink kinesisStreamSink = new KinesisStreamSink();
    kinesisStreamSink.configure(context);
    kinesisStreamSink.setMockClient(mockAmazonKinesisClient);
    kinesisStreamSink.setChannel(mockChannel);
    kinesisStreamSink.start();
    Status status = kinesisStreamSink.process();
    kinesisStreamSink.stop();
    Assert.assertEquals(Status.BACKOFF, status);
  }

  @Test(expected = IllegalStateException.class)
  public void testConfigureWithoutStreamName() throws EventDeliveryException {
    Context context = new Context();
    context.put(CONFIG_KEY_AWS_REGION, MOCK_REGION);
    KinesisStreamSink kinesisStreamSink = new KinesisStreamSink();
    kinesisStreamSink.configure(context);
  }

  @Test(expected = IllegalStateException.class)
  public void testConfigureWithoutRegionName() throws EventDeliveryException {
    Context context = new Context();
    context.put(CONFIG_KEY_STREAM_NAME, MOCK_STREAM_NAME);
    KinesisStreamSink kinesisStreamSink = new KinesisStreamSink();
    kinesisStreamSink.configure(context);
  }
}
