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
package org.apache.flume.source.cps;

import static org.apache.flume.source.cps.CloudPubSubSourceContstants.BATCH_SIZE;
import static org.apache.flume.source.cps.CloudPubSubSourceContstants.CONNECT_TIMEOUT;
import static org.apache.flume.source.cps.CloudPubSubSourceContstants.DEFAULT_BATCH_SIZE;
import static org.apache.flume.source.cps.CloudPubSubSourceContstants.DEFAULT_CONNECT_TIMEOUT;
import static org.apache.flume.source.cps.CloudPubSubSourceContstants.DEFAULT_READ_TIMEOUT;
import static org.apache.flume.source.cps.CloudPubSubSourceContstants.DEFAULT_RETRY_INTERVAL;
import static org.apache.flume.source.cps.CloudPubSubSourceContstants.HEADERS_PREFIX;
import static org.apache.flume.source.cps.CloudPubSubSourceContstants.MAX_SLEEP_INTERVAL;
import static org.apache.flume.source.cps.CloudPubSubSourceContstants.READ_TIMEOUT;
import static org.apache.flume.source.cps.CloudPubSubSourceContstants.SERVICE_ACCOUNT_KEY_PATH;
import static org.apache.flume.source.cps.CloudPubSubSourceContstants.SUBSCRIPTION;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class CloudPubSubSource extends AbstractSource implements PollableSource, Configurable {
  private static final Logger logger = LoggerFactory.getLogger(CloudPubSubSource.class);

  private int batchSize;
  private String subscriptionName;
  private Map<String, String> headersMap;
  private SourceCounter sourceCounter;
  private PullRequest pullRequest;
  private Pubsub pubsub;
  private int retryInterval;
  private long backoffSleepIncrement;
  private long maxBackoffSleep;

  public void configure(Context context) {
    subscriptionName = context.getString(SUBSCRIPTION);
    Preconditions.checkState(subscriptionName != null, SUBSCRIPTION + " is empty.");
    logger.info("subscrptionName : {}", subscriptionName);
    String serviceAccountKeyPath = context.getString(SERVICE_ACCOUNT_KEY_PATH);
    logger.info(SERVICE_ACCOUNT_KEY_PATH + ": {}", serviceAccountKeyPath);
    Preconditions.checkState(serviceAccountKeyPath != null, SERVICE_ACCOUNT_KEY_PATH + " is empty.");
    int connectTimeout = context.getInteger(CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
    Preconditions.checkState(0 <= connectTimeout, "You must set Positive number to " + CONNECT_TIMEOUT);
    int readTimeout = context.getInteger(READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
    Preconditions.checkState(0 <= readTimeout, "You must set Positive number to " + READ_TIMEOUT);

    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    Preconditions.checkState(0 <= batchSize, "You must set Positive number to " + batchSize);

    headersMap = context.getSubProperties(HEADERS_PREFIX);
    logger.info("headersMap : {}", headersMap);
    pullRequest = new PullRequest().setReturnImmediately(false).setMaxMessages(batchSize);
    this.backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
        PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
    this.maxBackoffSleep = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
        PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
    sourceCounter = new SourceCounter(getName());
    retryInterval = DEFAULT_RETRY_INTERVAL;
    try {
      pubsub = createPubsubClient(serviceAccountKeyPath, connectTimeout, readTimeout);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create pubsub client", e);
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    sourceCounter.start();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    try {
      fetchCPSEvent();
      TimeUnit.MILLISECONDS.sleep(retryInterval);
    } catch (Throwable t) {
      logger.error("Failed to fetch Event", t);
      status = Status.BACKOFF;
    }
    retryInterval = DEFAULT_RETRY_INTERVAL;
    return status;
  }

  @Override
  public synchronized void stop() {
    sourceCounter.stop();
    super.stop();
  }

  private void fetchCPSEvent() throws IOException, InterruptedException {
    while (true) {
      int fetchCount = pullEvent();
      if (fetchCount < batchSize)
        break;
    }
  }

  private int pullEvent() throws IOException, InterruptedException {
    List<Event> events = new ArrayList<>(batchSize);
    List<ReceivedMessage> receivedMessages = null;
    try {
      PullResponse pullResponse = pubsub.projects().subscriptions().pull(subscriptionName, pullRequest).execute();
      receivedMessages = pullResponse.getReceivedMessages();
    } catch (SocketTimeoutException e) {
      logger.info("Read timeout, subscription = {}", subscriptionName);
    }
    if (receivedMessages == null || receivedMessages.isEmpty()) {
      return 0;
    }
    List<String> ackIds = new ArrayList<>(receivedMessages.size());
    for (ReceivedMessage receivedMessage : receivedMessages) {
      if (receivedMessage.getMessage() != null) {
        PubsubMessage pubsubMessage = receivedMessage.getMessage();
        byte[] body = pubsubMessage.decodeData();
        if (body != null) {
          Event event = EventBuilder.withBody(body, headersMap);
          events.add(event);
        }
        ackIds.add(receivedMessage.getAckId());
      }
    }
    if (ackIds.size() == 0)
      return 0;
    sourceCounter.addToEventReceivedCount(ackIds.size());
    sourceCounter.incrementAppendBatchReceivedCount();
    try {
      getChannelProcessor().processEventBatch(events);
    } catch (ChannelException e) {
      TimeUnit.MILLISECONDS.sleep(retryInterval);
      retryInterval = retryInterval << 1;
      retryInterval = Math.min(retryInterval, MAX_SLEEP_INTERVAL);
      return 0;
    }
    AcknowledgeRequest ackRequest = new AcknowledgeRequest().setAckIds(ackIds);
    pubsub.projects().subscriptions().acknowledge(subscriptionName, ackRequest).execute();
    sourceCounter.addToEventAcceptedCount(events.size());
    sourceCounter.incrementAppendBatchAcceptedCount();
    return events.size();
  }

  private static Pubsub createPubsubClient(String privateKeyPath, int connectTimeout, int readTimeout)
      throws IOException, GeneralSecurityException, URISyntaxException {
    ClassLoader classLoader = CloudPubSubSource.class.getClassLoader();

    GoogleCredential credential;
    credential = GoogleCredential.fromStream(classLoader.getResourceAsStream(privateKeyPath));
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(PubsubScopes.all());
    }
    HttpRequestInitializer initializer = new CPSHttpInitializer(credential, connectTimeout, readTimeout);
    return new Pubsub.Builder(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(), initializer).build();
  }

  @Override
  public long getBackOffSleepIncrement() {
    return backoffSleepIncrement;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return maxBackoffSleep;
  }

  @VisibleForTesting
  public Map<String, String> getHeadersMap() {
    return headersMap;
  }

}
