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
package org.apache.flume.channel.kafka;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.shared.kafka.test.KafkaPartitionTestUtil;
import org.apache.flume.shared.kafka.test.PartitionOption;
import org.apache.flume.shared.kafka.test.PartitionTestScenario;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.PARTITION_HEADER_NAME;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.STATIC_PARTITION_CONF;

public class TestPartitions extends TestKafkaChannelBase {

  @Test
  public void testPartitionHeaderSet() throws Exception {
    doPartitionHeader(PartitionTestScenario.PARTITION_ID_HEADER_ONLY);
  }

  @Test
  public void testPartitionHeaderNotSet() throws Exception {
    doPartitionHeader(PartitionTestScenario.NO_PARTITION_HEADERS);
  }

  @Test
  public void testStaticPartitionAndHeaderSet() throws Exception {
    doPartitionHeader(PartitionTestScenario.STATIC_HEADER_AND_PARTITION_ID);
  }

  @Test
  public void testStaticPartitionHeaderNotSet() throws Exception {
    doPartitionHeader(PartitionTestScenario.STATIC_HEADER_ONLY);
  }

  @Test
  public void testPartitionHeaderMissing() throws Exception {
    doPartitionErrors(PartitionOption.NOTSET);
  }

  @Test(expected = org.apache.flume.ChannelException.class)
  public void testPartitionHeaderOutOfRange() throws Exception {
    doPartitionErrors(PartitionOption.VALIDBUTOUTOFRANGE);
  }

  @Test(expected = org.apache.flume.ChannelException.class)
  public void testPartitionHeaderInvalid() throws Exception {
    doPartitionErrors(PartitionOption.NOTANUMBER);
  }

  /**
   * This method tests both the default behavior (usePartitionHeader=false)
   * and the behaviour when the partitionId setting is used.
   * Under the default behaviour, one would expect an even distribution of
   * messages to partitions, however when partitionId is used we manually create
   * a large skew to some partitions and then verify that this actually happened
   * by reading messages directly using a Kafka Consumer.
   *
   * @param scenario
   * @throws Exception
   */
  private void doPartitionHeader(PartitionTestScenario scenario) throws Exception {
    final int numPtns = DEFAULT_TOPIC_PARTITIONS;
    final int numMsgs = numPtns * 10;
    final Integer staticPtn = DEFAULT_TOPIC_PARTITIONS - 2;
    Context context = prepareDefaultContext(false);
    if (scenario == PartitionTestScenario.PARTITION_ID_HEADER_ONLY ||
        scenario == PartitionTestScenario.STATIC_HEADER_AND_PARTITION_ID) {
      context.put(PARTITION_HEADER_NAME, "partition-header");
    }
    if (scenario == PartitionTestScenario.STATIC_HEADER_AND_PARTITION_ID ||
        scenario == PartitionTestScenario.STATIC_HEADER_ONLY) {
      context.put(STATIC_PARTITION_CONF, staticPtn.toString());
    }
    final KafkaChannel channel = createChannel(context);
    channel.start();

    // Create a map of PartitionId:List<Messages> according to the desired distribution
    // Initialise with empty ArrayLists
    Map<Integer, List<Event>> partitionMap = new HashMap<>(numPtns);
    for (int i = 0; i < numPtns; i++) {
      partitionMap.put(i, new ArrayList<Event>());
    }
    Transaction tx = channel.getTransaction();
    tx.begin();

    List<Event> orderedEvents = KafkaPartitionTestUtil.generateSkewedMessageList(scenario, numMsgs,
        partitionMap, numPtns, staticPtn);

    for (Event event : orderedEvents) {
      channel.put(event);
    }

    tx.commit();

    Map<Integer, List<byte[]>> resultsMap = KafkaPartitionTestUtil.retrieveRecordsFromPartitions(
        topic, numPtns, channel.getConsumerProps());

    KafkaPartitionTestUtil.checkResultsAgainstSkew(scenario, partitionMap, resultsMap, staticPtn,
        numMsgs);

    channel.stop();
  }

  /**
   * This function tests three scenarios:
   * 1. PartitionOption.VALIDBUTOUTOFRANGE: An integer partition is provided,
   * however it exceeds the number of partitions available on the topic.
   * Expected behaviour: ChannelException thrown.
   * <p>
   * 2. PartitionOption.NOTSET: The partition header is not actually set.
   * Expected behaviour: Exception is not thrown because the code avoids an NPE.
   * <p>
   * 3. PartitionOption.NOTANUMBER: The partition header is set, but is not an Integer.
   * Expected behaviour: ChannelExeption thrown.
   *
   * @param option
   * @throws Exception
   */
  private void doPartitionErrors(PartitionOption option) throws Exception {
    Context context = prepareDefaultContext(false);
    context.put(PARTITION_HEADER_NAME, KafkaPartitionTestUtil.PARTITION_HEADER);
    String tempTopic = findUnusedTopic();
    createTopic(tempTopic, 5);
    final KafkaChannel channel = createChannel(context);
    channel.start();

    Transaction tx = channel.getTransaction();
    tx.begin();

    Map<String, String> headers = new HashMap<>();
    switch (option) {
      case VALIDBUTOUTOFRANGE:
        headers.put(KafkaPartitionTestUtil.PARTITION_HEADER,
            String.valueOf(DEFAULT_TOPIC_PARTITIONS + 2));
        break;
      case NOTSET:
        headers.put("wrong-header", "2");
        break;
      case NOTANUMBER:
        headers.put(KafkaPartitionTestUtil.PARTITION_HEADER, "not-a-number");
        break;
      default:
        break;
    }

    Event event = EventBuilder.withBody(String.valueOf(9).getBytes(), headers);

    channel.put(event);

    tx.commit();

    deleteTopic(tempTopic);
  }
}
