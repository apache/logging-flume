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

import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.CONFIG_DEFAULT_VALUE_CONFIG_TYPE;
import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.CONFIG_KEY_AWS_ACCESS_KEY;
import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.CONFIG_KEY_AWS_REGION;
import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.CONFIG_KEY_AWS_SECRET_KEY;
import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.CONFIG_KEY_CREDENTIAL_TYPE;
import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.*;
import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.CONFIG_KEY_STREAM_NAME;
import static org.apache.flume.sink.kinesis.KinesisStreamSinkConstants.HEADER_KEY_PARTITION_KEY;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class KinesisStreamSink extends AbstractSink implements Configurable {
  private static final Logger logger = LoggerFactory.getLogger(KinesisStreamSink.class);
  private AmazonKinesisClient client;
  private String streamName;
  private SinkCounter counter;

  @Override
  public Status process() throws EventDeliveryException {
    Status result = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = null;
    Event event = null;
    try {
      transaction = channel.getTransaction();
      transaction.begin();

      PutRecordRequest recordRequest = new PutRecordRequest();
      recordRequest.setStreamName(streamName);

      event = channel.take();
      if (event == null) {
        transaction.commit();
        return Status.BACKOFF;
      }
      counter.incrementEventDrainAttemptCount();

      if (logger.isDebugEnabled()) {
        logger.debug("{Event} " + new String(event.getBody(), "UTF-8"));
      }

      // convert a flume event to kinesis message
      recordRequest.setData(ByteBuffer.wrap(event.getBody()));
      if (event.getHeaders().containsKey(HEADER_KEY_PARTITION_KEY)) {
        recordRequest.setPartitionKey(event.getHeaders().get(HEADER_KEY_PARTITION_KEY));
      } else {
        recordRequest.setPartitionKey(UUID.randomUUID().toString());
      }
      client.putRecord(recordRequest);
      counter.incrementEventDrainSuccessCount();
      transaction.commit();
    } catch (Exception ex) {
      String errorMsg = "Failed to publish events";
      logger.error(errorMsg, ex);
      result = Status.BACKOFF;
      if (transaction != null) {
        try {
          transaction.rollback();
        } catch (Exception e) {
          logger.error("Transaction rollback failed", e);
          throw Throwables.propagate(e);
        }
      }
      throw new EventDeliveryException(errorMsg, ex);
    } finally {
      if (transaction != null) {
        transaction.close();
      }
    }
    return result;
  }

  @Override
  public void configure(Context context) {
    streamName = context.getString(CONFIG_KEY_STREAM_NAME);
    Preconditions.checkState(streamName != null, "You must set " + CONFIG_KEY_AWS_SECRET_KEY);
    client = createClient(context);
    String regionName = context.getString(CONFIG_KEY_AWS_REGION);
    Preconditions.checkState(regionName != null, "You must set " + CONFIG_KEY_AWS_REGION);
    Regions region = Regions.fromName(regionName);
    client.setRegion(Region.getRegion(region));
    counter = new SinkCounter(getName());
  }

  private static AmazonKinesisClient createClient(Context context) {
    String credentialTypeString = context.getString(CONFIG_KEY_CREDENTIAL_TYPE,
        CONFIG_DEFAULT_VALUE_CONFIG_TYPE);
    CredentialType credentialType = CredentialType.valueOf(credentialTypeString);
    switch (credentialType) {
      case basic:
        String awsAccessKey = context.getString(CONFIG_KEY_AWS_ACCESS_KEY);
        String awsSecretKey = context.getString(CONFIG_KEY_AWS_SECRET_KEY);
        Preconditions.checkState(awsAccessKey != null && awsAccessKey != null,
            "You must set required informations of " + CredentialType.basic + " credientials");
        return new AmazonKinesisClient(new BasicAWSCredentials(awsAccessKey, awsSecretKey));
      case session:
        int durationSeconds = context.getInteger(CONFIG_KEY_AWS_SESSION_DURATION_SECONDS,
            CONFIG_DEFAULT_SESSION_DURATION_SECONDS);
        // DurationSeconds parameters must be set between 900 and 129600.
        Preconditions.checkState(durationSeconds < 900 || 129600 < durationSeconds,
            "You must set required informations of " + CredentialType.session + " credientials");
        return new AmazonKinesisClient(createBasicSessionCredentials(durationSeconds));
      default:
        return new AmazonKinesisClient();
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    counter.start();
  }

  @Override
  public synchronized void stop() {
    client.shutdown();
    counter.stop();
    super.stop();
  }

  void setMockClient(AmazonKinesisClient mockClient) {
    this.client = mockClient;
  }

  private static BasicSessionCredentials createBasicSessionCredentials(int durationSeconds) {
    AWSSecurityTokenServiceClient stsClient = new AWSSecurityTokenServiceClient(
        new ProfileCredentialsProvider());
    GetSessionTokenRequest getSessionTokenRequest = new GetSessionTokenRequest();
    getSessionTokenRequest.setDurationSeconds(durationSeconds);
    GetSessionTokenResult sessionTokenResult = stsClient.getSessionToken(getSessionTokenRequest);
    Credentials sessionCredentials = sessionTokenResult.getCredentials();

    return new BasicSessionCredentials(sessionCredentials.getAccessKeyId(),
        sessionCredentials.getSecretAccessKey(), sessionCredentials.getSessionToken());
  }
}
